package file

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"sidney.kochman.org/cloudblock"
)

func TestWriteRead(t *testing.T) {
	dir, err := ioutil.TempDir("", "cloudblock-backend-file-")
	if err != nil {
		t.Fatalf("unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	f, err := NewBackend(dir)
	if err != nil {
		t.Fatalf("unable to create file backend: %v", err)
	}

	// make sure we conform to the interface
	var fh cloudblock.Handle

	// 1 gigabyte, 100 byte bands
	fh, err = f.New("test-write-read", 1000*1000*1000, 100)
	if err != nil {
		t.Fatalf("unable to create handle: %v", err)
	}

	// simple write at the beginning
	err = fh.WriteAt([]byte("hello"), 0)
	if err != nil {
		t.Errorf("unable to write: %v", err)
	}
	p := make([]byte, 5)
	err = fh.ReadAt(p, 0)
	if err != nil {
		t.Errorf("unable to read: %v", err)
	}
	expected := []byte("hello")
	if !bytes.Equal(p, expected) {
		t.Errorf("expected %v, got %v", expected, p)
	}

	// write with offset. zeroes should be filled in on either side
	err = fh.WriteAt([]byte("hello"), 50)
	if err != nil {
		t.Errorf("unable to write: %v", err)
	}
	p = make([]byte, 7)
	err = fh.ReadAt(p, 49)
	if err != nil {
		t.Errorf("unable to read: %v", err)
	}
	expected = append(append([]byte{0}, []byte("hello")...), byte(0))
	if !bytes.Equal(p, expected) {
		t.Errorf("expected %v, got %v", expected, p)
	}

	// write with offset in other band
	err = fh.WriteAt([]byte("hello"), 1050)
	if err != nil {
		t.Errorf("unable to write: %v", err)
	}
	p = make([]byte, 5)
	err = fh.ReadAt(p, 1050)
	if err != nil {
		t.Errorf("unable to read: %v", err)
	}
	expected = []byte("hello")
	if !bytes.Equal(p, expected) {
		t.Errorf("expected %v, got %v", expected, p)
	}

	// write with offset across bands
	err = fh.WriteAt([]byte("hello"), 1997)
	if err != nil {
		t.Errorf("unable to write: %v", err)
	}
	p = make([]byte, 5)
	err = fh.ReadAt(p, 1997)
	if err != nil {
		t.Errorf("unable to read: %v", err)
	}
	expected = []byte("hello")
	if !bytes.Equal(p, expected) {
		t.Errorf("expected %v, got %v", expected, p)
	}
}
