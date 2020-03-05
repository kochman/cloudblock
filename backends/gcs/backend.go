package gcs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/kochman/cloudblock"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Backend struct {
	b *storage.BucketHandle
}

func NewBackend(bucket string) (*Backend, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile("/Users/sidney/Work/cloudblock/marine-cycle-160323-e681654dbc52.json"))
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
		return nil, cloudblock.HandleExists
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
		fm:        map[uint64]chan struct{}{},
		bm:        map[uint64]*sync.Mutex{},
		bcm:       map[uint64][]byte{},
		gc:        newGCSCacher(b.b),
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
		fm:        map[uint64]chan struct{}{},
		bm:        map[uint64]*sync.Mutex{},
		bcm:       map[uint64][]byte{},
		gc:        newGCSCacher(b.b),
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
			return fmt.Errorf("unable to delete [%s]: %w", obj.ObjectName(), err)
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

	// finalizer
	fl sync.Mutex
	fm map[uint64]chan struct{}

	// band locks
	// we only want one thing operating on a band at a time
	bl sync.Mutex
	bm map[uint64]*sync.Mutex

	// band cache
	bcl sync.Mutex
	bcm map[uint64][]byte

	gc *gcsCacher
}

func (h *Handle) lockBand(band uint64) {
	h.bl.Lock()
	l, ok := h.bm[band]
	if !ok {
		l = &sync.Mutex{}
		h.bm[band] = l
	}
	h.bl.Unlock()
	l.Lock()
}

func (h *Handle) unlockBand(band uint64) {
	h.bl.Lock()
	h.bm[band].Unlock()
	h.bl.Unlock()
}

func (h *Handle) cacheBand(band uint64, b []byte) {
	h.bcl.Lock()
	h.bcm[band] = b
	h.bcl.Unlock()
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

// TODO: don't let a blocker grow indefinitely
type blocker struct {
	l sync.Mutex
	m map[uint64]<-chan time.Time
}

func (b *blocker) Block(k uint64) {
	b.l.Lock()
	ch, ok := b.m[k]
	if !ok {
		// set up a limiter
		ch = time.Tick(1500 * time.Millisecond)
		b.m[k] = ch
	}
	b.l.Unlock()
	<-ch
}

var bandBlocker = blocker{m: map[uint64]<-chan time.Time{}}

func (h *Handle) requestFinalization(band uint64) {
	h.fl.Lock()
	w, ok := h.fm[band]
	if !ok {
		w = make(chan struct{}, 1)
		h.fm[band] = w
		go func() {
			for range w {
				h.lockBand(band)
				err := h.finalizeTransactionsForBand(band)
				if err != nil {
					log.Printf("background finalization error: %v", err)
				}
				h.unlockBand(band)
			}
		}()
	}
	h.fl.Unlock()

	// we only want to request a finalization if someone else hasn't already
	select {
	case w <- struct{}{}:
	default:
	}
}

func (h *Handle) finalizeTransactionsForBand(band uint64) error {
	start := time.Now()
	ctx := context.Background()

	// discover any in-flight transactions
	idxStr := strconv.FormatUint(band, 36)
	txnPrefix := h.prefix + "/txns/" + idxStr + "-"
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
		if len(split) != 2 {
			return fmt.Errorf("malformed transaction key")
		}

		txn, err := strconv.ParseUint(split[0], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse transaction ID: %w", err)
		}

		offset, err := strconv.ParseUint(split[1], 10, 64)
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

	if len(txns) == 0 {
		return nil
	}

	// we need to put these in numeric order by txn ID because GCS does lexicographic
	sort.Slice(txns, func(i, j int) bool {
		return txns[i].id < txns[j].id
	})

	// get the old stuff
	new := false
	dest := fmt.Sprintf("%s/bands/%s", h.prefix, idxStr)
	r, err := h.gc.Reader(dest)
	if err == storage.ErrObjectNotExist {
		new = true
	} else if err != nil {
		return fmt.Errorf("unable to get band reader: %w", err)
	}

	var merged []byte
	if !new {
		merged, err = ioutil.ReadAll(r)
		if err != nil {
			return fmt.Errorf("unable to read existing data: %w", err)
		}
	}

	// write new data on top of the bands
	for _, t := range txns {
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

		// figure out if we need to write some padding in the beginning
		for uint64(len(merged)) < t.offset {
			merged = append(merged, 0)
		}

		// overwrite existing data with new bytes, or append if nothing underneath
		for i := uint64(0); i < uint64(len(p)); i++ {
			if t.offset+i+1 > uint64(len(merged)) {
				merged = append(merged, p[i])
			} else {
				merged[t.offset+i] = p[i]
			}
		}
	}

	// wait for rate limit to settle
	bandBlocker.Block(band)

	// write the band back out
	f := h.b.Object(dest).NewWriter(ctx)
	_, err = f.Write(merged)
	if err != nil {
		return fmt.Errorf("unable to write to band: %w", err)
	}

	err = f.Close()
	if err != nil {
		return fmt.Errorf("unable to close writer: %w", err)
	}

	h.gc.Put(dest, merged)

	// delete all the transactions after we've written them to the band.
	// it's okay if we die after writing above and before deleting below,
	// since replaying these transactions will result in the same state.
	for _, t := range txns {
		obj := h.b.Object(t.key)
		err = obj.Delete(ctx)
		if err != nil {
			return fmt.Errorf("unable to delete transaction: %w", err)
		}
	}

	log.Printf("finalize %s took %v", idxStr, time.Since(start))

	return nil
}

func (h *Handle) finalizeTransactions() error {
	ctx := context.Background()

	// discover any in-flight transactions
	txnPrefix := h.prefix + "/txns/"
	q := &storage.Query{Prefix: txnPrefix}
	err := q.SetAttrSelection([]string{"Name"})
	if err != nil {
		return fmt.Errorf("unable to set attribute selection: %w", err)
	}

	bands := map[uint64]struct{}{}
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
		if _, ok := bands[band]; !ok {
			bands[band] = struct{}{}
		}
	}

	errs := make(chan error, len(bands))
	wg := sync.WaitGroup{}
	for band := range bands {
		wg.Add(1)
		go func(band uint64) {
			defer wg.Done()

			h.lockBand(band)
			defer h.unlockBand(band)
			err := h.finalizeTransactionsForBand(band)
			if err != nil {
				errs <- fmt.Errorf("unable to finalize transactions for band: %w", err)
			}
		}(band)
	}

	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}

	return nil
}

func (h *Handle) Close() error {
	return nil
}

func (h *Handle) ReadAt(p []byte, offset uint64) error {
	if offset+uint64(len(p)) > h.size {
		panic("read outside bounds")
	}
	// determine which band we're reading from
	idx := offset / h.blockSize
	idxStr := strconv.FormatUint(idx, 36)
	src := fmt.Sprintf("%s/bands/%s", h.prefix, idxStr)

	// adjust offset to account for band
	off := offset % h.blockSize

	h.lockBand(idx)
	defer h.unlockBand(idx)

	// for now, always finalize all transactions before reading.
	// this is sloooooow. we should either:
	//  - only finalize transactions for this band, or
	//  - layer transactions on top of the band, if any, returning a view into
	//    the state we would have if transactions were finalized
	err := h.finalizeTransactionsForBand(idx)
	if err != nil {
		return fmt.Errorf("unable to finalize transactions: %w", err)
	}

	// does this read need to wrap?
	readLen := uint64(len(p))
	if off+readLen >= h.blockSize {
		readLen = h.blockSize - off
	}
	log.Printf("readLen: %d", readLen)
	b := make([]byte, readLen)

	// get a reader for the specific chunk of the band we need
	bandExists := true
	f, err := h.gc.Reader(src)
	if err == storage.ErrObjectNotExist {
		// it's just zeros
		bandExists = false
	} else if err != nil {
		return fmt.Errorf("unable to get band reader: %w", err)
	}

	if bandExists {
		buf := bufio.NewReader(f)
		_, err := buf.Discard(int(off))
		if err != nil {
			if err == io.EOF {
				// it's empty!
			} else {
				return fmt.Errorf("unable to discard from band: %w", err)
			}
		}
		n, err := buf.Read(b)
		if err != nil {
			if err == io.EOF {
				// it's zeros
				for i := n; i < len(b); i++ {
					b[i] = 0
				}
			} else {
				return fmt.Errorf("unable to read from band: %w", err)
			}
		}
	} else {
		// it's zeros
		for i := 0; i < len(b); i++ {
			b[i] = 0
		}
	}

	copy(p, b)

	// continue reading from the next band if requested
	if uint64(len(p))-readLen > 0 {
		log.Printf("READING ADDITIONAL")

		rem := make([]byte, uint64(len(p))-readLen)
		err = h.ReadAt(rem, offset+uint64(len(b)))
		if err != nil {
			return err
		}

		copy(p[len(b):], rem)
		log.Printf("spliced: [%s]", p)
	}

	return nil
}

// WriteAt always writes transactions
func (h *Handle) WriteAt(p []byte, offset uint64) error {
	if offset+uint64(len(p)) > h.size {
		panic("write outside bounds")
	}
	ctx := context.Background()

	// adjust offset to account for band
	off := offset % h.blockSize

	// start a transaction
	txn := atomic.AddUint64(&h.txn, 1)

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

	h.requestFinalization(idx)

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

	h.lockBand(idx)
	defer h.unlockBand(idx)

	dest := fmt.Sprintf("%s/bands/%s", h.prefix, strconv.FormatUint(idx, 36))
	f := h.b.Object(dest).NewWriter(ctx)

	// does this write need to wrap?
	var rem []byte
	if off+uint64(len(p)) >= h.blockSize {
		rem = p[h.blockSize-off:]
		p = p[:h.blockSize-off]
	}

	// get the old stuff and write the new stuff on top
	new := false
	r, err := h.b.Object(dest).NewReader(ctx)
	if err == storage.ErrObjectNotExist {
		new = true
	} else if err != nil {
		return fmt.Errorf("unable to get band reader: %w", err)
	} else {
		defer r.Close()
	}

	var merged []byte
	if new {
		// need to pad beginning with zeros
		merged = make([]byte, off)
	} else {
		merged, err = ioutil.ReadAll(r)
		if err != nil {
			return fmt.Errorf("unable to read existing data: %w", err)
		}
	}

	// overwrite existing data with new bytes, or append if nothing underneath
	for i := uint64(0); i < uint64(len(p)); i++ {
		if off+i+1 > uint64(len(merged)) {
			merged = append(merged, p[i])
		} else {
			merged[off+i] = p[i]
		}
	}

	_, err = f.Write(merged)
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

type gcsCacher struct {
	b *storage.BucketHandle
	l sync.Mutex
	c map[string][]byte
}

// TODO: this should probably be closeable
func (gc *gcsCacher) Reader(key string) (io.Reader, error) {
	gc.l.Lock()
	defer gc.l.Unlock()

	if b, ok := gc.c[key]; ok {
		c := make([]byte, len(b))
		copy(c, b)
		return bytes.NewReader(c), nil
	}
	return gc.b.Object(key).NewReader(context.Background())
}

func (gc *gcsCacher) Put(key string, b []byte) {
	c := make([]byte, len(b))
	copy(c, b)

	gc.l.Lock()
	gc.c[key] = c
	gc.l.Unlock()
}

func newGCSCacher(b *storage.BucketHandle) *gcsCacher {
	return &gcsCacher{
		b: b,
		c: map[string][]byte{},
	}
}
