package cloudblock

import "log"

type File struct {
	id        string
	size      uint64
	blockSize uint64
}

type Backend interface {
	New(id string, size, blockSize uint64) (*Handle, error)
	Open(id string) (*Handle, error)
}

type Handle interface {
	Close() error
	ReadAt(b []byte, offset uint64) error
	WriteAt(b []byte, offset uint64) error

	Size() (uint64, error)
}

func main() {
	log.Print("main")
}
