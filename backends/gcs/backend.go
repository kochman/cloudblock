package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Backend struct {
	b *storage.BucketHandle
}

func NewBackend(bucket string) (*Backend, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile("/Users/sidney/Projects/cloudblock/cloudblock-267721-f144a49fe420.json"))
	if err != nil {
		return nil, fmt.Errorf("unable to create client: %w", err)
	}

	b := client.Bucket(bucket)
	_, err = b.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get bucket handle: %w", err)
	}

	backend := &Backend{
		b: b,
	}
	return backend, nil
}

func (b *Backend) New(id string, size, blockSize uint64) (*Handle, error) {
	ctx := context.Background()

	// make sure this doesn't exist
	obj := b.b.Object(id + "/manifest.json")
	_, err := obj.Attrs(ctx)
	if err != storage.ErrObjectNotExist {
		return nil, fmt.Errorf("manifest already exists")
	}

	// write a manifest with some info about this file
	mw := obj.NewWriter(ctx)
	m := manifest{Size: size, BlockSize: blockSize}
	enc := json.NewEncoder(mw)
	err = enc.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("unable to write manifest: %w", err)
	}
	err = mw.Close()
	if err != nil {
		return nil, fmt.Errorf("unable to close manifest: %w", err)
	}

	h := &Handle{
		b:         b.b,
		prefix:    id,
		size:      size,
		blockSize: blockSize,
	}

	return h, nil
}

func (b *Backend) Open(id string) (*Handle, error) {
	ctx := context.Background()

	// read info from manifest
	mo, err := b.b.Object(id + "/manifest.json").NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get manifest reader: %w", err)
	}
	defer mo.Close()
	m := manifest{}
	enc := json.NewDecoder(mo)
	err = enc.Decode(&m)
	if err != nil {
		return nil, fmt.Errorf("unable to read manifest: %w", err)
	}

	h := &Handle{
		b:         b.b,
		prefix:    id,
		size:      m.Size,
		blockSize: m.BlockSize,
	}

	err = h.finalizeTransactions()
	if err != nil {
		return nil, fmt.Errorf("unable to finalize transactions: %w", err)
	}

	return h, nil
}

// Delete is on the backend instead of the Handle because it should be possible
// to ensure files are deleted without acquiring a handle.
func (b *Backend) Delete(id string) error {
	ctx := context.Background()

	// make sure this doesn't exist
	q := &storage.Query{Prefix: id}
	err := q.SetAttrSelection([]string{"Name"})
	if err != nil {
		return fmt.Errorf("unable to set attribute selection: %w", err)
	}
	it := b.b.Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return fmt.Errorf("unable to iterate: %w", err)
		}

		obj := b.b.Object(attrs.Name)
		err = obj.Delete(ctx)
		if err != nil {
			return fmt.Errorf("unable to delete [%s]", obj.ObjectName())
		}
	}
	return nil
}

type Handle struct {
	prefix    string
	size      uint64
	blockSize uint64

	b *storage.BucketHandle

	// TODO: make sure this never wraps
	txn uint64
}

type manifest struct {
	Size      uint64
	BlockSize uint64
}

type transaction struct {
	// transactions have a band so that any read against that band can quickly
	// determine if it has an in-flight transaction
	band uint64

	// id is a monotonically-increasing number so that transactions are applied
	// to bands in order. it comes from `Handle.txn`
	id uint64

	// offset is within the band
	offset uint64

	// key is the full key for the transaction object in the bucket
	key string
}

// GCS only wants one write/obj/sec
var bandRateLimiter = map[uint64]func(){}

func (h *Handle) finalizeTransactions() error {
	ctx := context.Background()

	// discover any in-flight transactions
	txnPrefix := h.prefix + "/txns/"
	q := &storage.Query{Prefix: txnPrefix}
	err := q.SetAttrSelection([]string{"Name"})
	if err != nil {
		return fmt.Errorf("unable to set attribute selection: %w", err)
	}

	txns := []transaction{}
	it := h.b.Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return fmt.Errorf("unable to iterate: %w", err)
		}

		key := attrs.Name

		split := strings.Split(key[len(txnPrefix):], "-")
		if len(split) != 3 {
			return fmt.Errorf("malformed transaction key")
		}

		band, err := strconv.ParseUint(split[0], 36, 64)
		if err != nil {
			return fmt.Errorf("unable to parse band: %w", err)
		}

		txn, err := strconv.ParseUint(split[1], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse transaction ID: %w", err)
		}

		offset, err := strconv.ParseUint(split[2], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse offset: %w", err)
		}

		t := transaction{
			band:   band,
			id:     txn,
			offset: offset,
			key:    key,
		}
		txns = append(txns, t)
	}
	// we need to put these in numeric order by txn ID because GCS does lexicographic
	sort.Slice(txns, func(i, j int) bool {
		return txns[i].id < txns[j].id
	})

	// update the bands
	for _, t := range txns {
		if limit, ok := bandRateLimiter[t.band]; ok {
			limit()
			// no point in deleting since we're about to just overwrite it anyway
		}

		obj := h.b.Object(t.key)

		f, err := obj.NewReader(ctx)
		if err != nil {
			return fmt.Errorf("unable to get transaction reader: %w", err)
		}
		defer f.Close()
		p, err := ioutil.ReadAll(f)
		if err != nil {
			return fmt.Errorf("unable to read from transaction: %w", err)
		}

		off := h.blockSize*t.band + t.offset

		err = h.writeToBand(p, off)
		if err != nil {
			return fmt.Errorf("unable to write transaction to band: %w", err)
		}

		timeout := time.After(time.Second)
		bandRateLimiter[t.band] = func() {
			<-timeout
		}

		// it's okay if we die after writing above and before deleting below,
		// since replaying this transaction will result in the same state.
		err = obj.Delete(ctx)
		if err != nil {
			return fmt.Errorf("unable to delete transaction: %w", err)
		}
	}

	return nil
}

func (h *Handle) Close() error {
	return nil
}

func (h *Handle) ReadAt(p []byte, offset uint64) error {
	// for now, always finalize all transactions before reading.
	// this is sloooooow. we should either:
	//  - only finalize transactions for this band, or
	//  - layer transactions on top of the band, if any, returning a view into
	//    the state we would have if transactions were finalized
	err := h.finalizeTransactions()
	if err != nil {
		return err
	}
	ctx := context.Background()

	// determine which band we're reading from
	idx := offset / h.blockSize
	idxStr := strconv.FormatUint(idx, 36)
	src := fmt.Sprintf("%s/bands/%s", h.prefix, idxStr)

	// adjust offset to account for band
	off := offset % h.blockSize

	// does this read need to wrap?
	var rem []byte
	if off+uint64(len(p)) >= h.blockSize {
		rem = p[h.blockSize-off:]
		p = p[:h.blockSize-off]
	}

	// get a reader for the specific chunk of the band we need
	f, err := h.b.Object(src).NewRangeReader(ctx, int64(off), int64(len(p)))
	if err != nil {
		return fmt.Errorf("unable to get band reader: %w", err)
	}
	defer f.Close()

	n, err := f.Read(p)
	if err != nil {
		if err == io.EOF {
			// it's zeros
			for i := len(p); i > n; i-- {
				p = append(p, 0)
			}
		} else {
			return fmt.Errorf("unable to read from band: %w", err)
		}
	}

	if rem != nil {
		err = h.ReadAt(rem, offset+uint64(len(p)))
		if err != nil {
			return err
		}
		p = append(p, rem...)
	}

	return nil
}

// WriteAt always writes transactions
func (h *Handle) WriteAt(p []byte, offset uint64) error {
	ctx := context.Background()

	// adjust offset to account for band
	off := offset % h.blockSize

	// start a transaction
	txn := h.txn
	h.txn++

	// determine which band this transaction is writing to
	idx := offset / h.blockSize
	dest := fmt.Sprintf("%s/txns/%s-%d-%d", h.prefix, strconv.FormatUint(idx, 36), txn, off)
	f := h.b.Object(dest).NewWriter(ctx)

	// does this write need to wrap?
	var rem []byte
	if off+uint64(len(p)) >= h.blockSize {
		rem = p[h.blockSize-off:]
		p = p[:h.blockSize-off]
	}

	_, err := f.Write(p)
	if err != nil {
		return fmt.Errorf("unable to write to transaction: %w", err)
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("unable to close transaction writer: %w", err)
	}

	if rem != nil {
		return h.WriteAt(rem, offset+uint64(len(p)))
	}

	return nil
}

// offset is from beginning of file
func (h *Handle) writeToBand(p []byte, offset uint64) error {
	ctx := context.Background()

	// adjust offset to account for band
	off := offset % h.blockSize

	// determine which band we're writing to
	idx := offset / h.blockSize
	dest := fmt.Sprintf("%s/bands/%s", h.prefix, strconv.FormatUint(idx, 36))
	f := h.b.Object(dest).NewWriter(ctx)

	// does this write need to wrap?
	var rem []byte
	if off+uint64(len(p)) >= h.blockSize {
		rem = p[h.blockSize-off:]
		p = p[:h.blockSize-off]
	}

	// need to pad beginning with zeros
	z := make([]byte, off)
	_, err := f.Write(z)
	if err != nil {
		return fmt.Errorf("unable to pad band: %w", err)
	}

	_, err = f.Write(p)
	if err != nil {
		return fmt.Errorf("unable to write to band: %w", err)
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("unable to close writer: %w", err)
	}

	if rem != nil {
		return h.writeToBand(rem, offset+uint64(len(p)))
	}

	return nil
}

func (h *Handle) Size() (uint64, error) {
	return h.size, nil
}
