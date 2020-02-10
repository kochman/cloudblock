package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"cloud.google.com/go/storage"
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

	// write a manifest with some info about this file
	mw := b.b.Object(id + "/manifest.json").NewWriter(ctx)
	m := manifest{Size: size, BlockSize: blockSize}
	enc := json.NewEncoder(mw)
	err := enc.Encode(m)
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
	return h, nil
}

type Handle struct {
	b         *storage.BucketHandle
	prefix    string
	size      uint64
	blockSize uint64
}

type manifest struct {
	Size      uint64
	BlockSize uint64
}

func (h *Handle) Close() error {
	return nil
}

func (h *Handle) ReadAt(p []byte, offset uint64) error {
	ctx := context.Background()

	// determine which band we're reading from
	idx := offset / h.blockSize
	src := fmt.Sprintf("%s/bands/%s", h.prefix, strconv.FormatUint(idx, 36))

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

func (h *Handle) WriteAt(p []byte, offset uint64) error {
	ctx := context.Background()

	// determine which band we're writing to
	idx := offset / h.blockSize
	dest := fmt.Sprintf("%s/bands/%s", h.prefix, strconv.FormatUint(idx, 36))
	f := h.b.Object(dest).NewWriter(ctx)

	// adjust offset to account for band
	off := offset % h.blockSize

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
		return h.WriteAt(rem, offset+uint64(len(p)))
	}

	return nil
}

func (h *Handle) Size() (uint64, error) {
	return h.size, nil
}
