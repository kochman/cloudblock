package file

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
)

func NewBackend(dir string) (*Backend, error) {
	_, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("unable to stat dir: %w", err)
	}

	b := &Backend{
		dir: dir,
	}
	return b, nil
}

type Backend struct {
	dir string
}

type manifest struct {
	Size      uint64
	BlockSize uint64
}

func (b *Backend) New(id string, size, blockSize uint64) (*Handle, error) {
	p := path.Join(b.dir, id)
	err := os.Mkdir(p, 0755)
	if err != nil {
		return nil, fmt.Errorf("unable to create dir: %w", err)
	}

	err = os.Mkdir(path.Join(p, "bands"), 0755)
	if err != nil {
		return nil, fmt.Errorf("unable to create bands dir: %w", err)
	}

	// write a manifest with some info about this file
	mf, err := os.OpenFile(path.Join(p, "manifest.json"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("unable to create manifest: %w", err)
	}
	defer mf.Close()
	m := manifest{Size: size, BlockSize: blockSize}
	enc := json.NewEncoder(mf)
	err = enc.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("unable to write manifest: %w", err)
	}

	h := &Handle{
		dir:       p,
		size:      size,
		blockSize: blockSize,
	}

	return h, nil
}

func (b *Backend) Open(id string) (*Handle, error) {
	p := path.Join(b.dir, id)
	_, err := os.Stat(p)
	if err != nil {
		return nil, fmt.Errorf("unable to stat dir: %w", err)
	}

	// read info from manifest
	mf, err := os.Open(path.Join(p, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("unable to open manifest: %w", err)
	}
	defer mf.Close()
	m := manifest{}
	enc := json.NewDecoder(mf)
	err = enc.Decode(&m)
	if err != nil {
		return nil, fmt.Errorf("unable to read manifest: %w", err)
	}

	h := &Handle{
		dir:       p,
		size:      m.Size,
		blockSize: m.BlockSize,
	}
	return h, nil
}

type Handle struct {
	dir       string
	size      uint64
	blockSize uint64
}

func (h *Handle) Close() error {
	return nil
}

func (h *Handle) ReadAt(p []byte, offset uint64) error {
	// determine which band we're reading from
	idx := offset / h.blockSize
	src := path.Join(h.dir, "bands", strconv.FormatUint(idx, 36))
	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("unable to open band: %w", err)
	}
	defer f.Close()

	// adjust offset to account for band
	off := offset % h.blockSize

	// does this read need to wrap?
	var rem []byte
	if off+uint64(len(p)) >= h.blockSize {
		rem = p[h.blockSize-off:]
		p = p[:h.blockSize-off]
	}

	n, err := f.ReadAt(p, int64(off))
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
	// determine which band we're writing to
	idx := offset / h.blockSize
	dest := path.Join(h.dir, "bands", strconv.FormatUint(idx, 36))
	f, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("unable to open band: %w", err)
	}
	defer f.Close()

	// adjust offset to account for band
	off := offset % h.blockSize

	// does this write need to wrap?
	var rem []byte
	if off+uint64(len(p)) >= h.blockSize {
		rem = p[h.blockSize-off:]
		p = p[:h.blockSize-off]
	}

	_, err = f.WriteAt(p, int64(off))
	if err != nil {
		return fmt.Errorf("unable to write to band: %w", err)
	}

	if rem != nil {
		return h.WriteAt(rem, offset+uint64(len(p)))
	}

	return nil
}

func (h *Handle) Size() (uint64, error) {
	return h.size, nil
}
