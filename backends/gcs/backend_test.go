package gcs

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/kochman/cloudblock"
)

func TestWriteRead(t *testing.T) {
	t.Parallel()

	f, err := NewBackend("repl-cloudblock")
	if err != nil {
		t.Fatalf("unable to create file backend: %v", err)
	}

	if f.Delete("test-write-read") != nil {
		t.Fatalf("unable to delete: %v", err)
	}
	time.Sleep(time.Second) // wait for rate limit to settle

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
		t.Fatalf("unable to write: %v", err)
	}
	p := make([]byte, 5)
	err = fh.ReadAt(p, 0)
	if err != nil {
		t.Fatalf("unable to read: %v", err)
	}
	expected := []byte("hello")
	if !bytes.Equal(p, expected) {
		t.Fatalf("expected %v, got %v", expected, p)
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

// TestEmptyRead makes sure that we read zeros even if there's no underlying
// band.
func TestEmptyRead(t *testing.T) {
	t.Parallel()

	f, err := NewBackend("repl-cloudblock")
	if err != nil {
		t.Fatalf("unable to create file backend: %v", err)
	}

	f.Delete("test-empty-read")
	time.Sleep(time.Second) // wait for rate limit to settle

	// make sure we conform to the interface
	var fh cloudblock.Handle

	// 1 gigabyte, 100 byte bands
	fh, err = f.New("test-empty-read", 1000*1000*1000, 100)
	if err != nil {
		t.Fatalf("unable to create handle: %v", err)
	}

	p := make([]byte, 100)
	err = fh.ReadAt(p, 0)
	if err != nil {
		t.Errorf("unable to read: %v", err)
	}
	err = fh.ReadAt(p, 1000*1000*1000-100)
	if err != nil {
		t.Errorf("unable to read: %v", err)
	}
	err = fh.ReadAt(p, 500*1000*1000-50)
	if err != nil {
		t.Errorf("unable to read: %v", err)
	}
}

func TestRepeatedWrites(t *testing.T) {
	t.Parallel()

	f, err := NewBackend("repl-cloudblock")
	if err != nil {
		t.Fatalf("unable to create file backend: %v", err)
	}

	f.Delete("test-repeated-writes")
	time.Sleep(time.Second) // wait for rate limit to settle

	// make sure we conform to the interface
	var fh cloudblock.Handle

	// 1 gigabyte, 100 byte bands
	fh, err = f.New("test-repeated-writes", 1000*1000*1000, 100)
	if err != nil {
		t.Fatalf("unable to create handle: %v", err)
	}

	// simple write at the beginning. if we error out here then we're probably
	// not respecting GCS rate limits.
	for i := 0; i < 50; i++ {
		err = fh.WriteAt([]byte("hello"), 0)
		if err != nil {
			t.Fatalf("unable to write: %v", err)
		}
		p := make([]byte, 5)
		err = fh.ReadAt(p, 0)
		if err != nil {
			t.Fatalf("unable to read: %v", err)
		}
		expected := []byte("hello")
		if !bytes.Equal(p, expected) {
			t.Fatalf("expected %v, got %v", expected, p)
		}
	}
}

func TestWritesEverywhere(t *testing.T) {
	t.Parallel()

	expected := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	f, err := NewBackend("repl-cloudblock")
	if err != nil {
		t.Fatalf("unable to create file backend: %v", err)
	}

	f.Delete("test-writes-everywhere")
	time.Sleep(time.Second) // wait for rate limit to settle

	// make sure we conform to the interface
	var fh cloudblock.Handle

	// 1 kilobyte, 55 byte bands
	const size = 1000
	fh, err = f.New("test-writes-everywhere", size, 55)
	if err != nil {
		t.Fatalf("unable to create handle: %v", err)
	}

	written := uint64(0)
	for {
		if written+uint64(len(expected)) > size {
			break
		}
		p := make([]byte, len(expected))
		copy(p, expected)
		err = fh.WriteAt(p, written)
		if err != nil {
			t.Fatalf("unable to write: %v", err)
		}
		written += uint64(len(expected))
	}

	// read it back
	read := uint64(0)
	for {
		if read+uint64(len(expected)) > size {
			break
		}
		p := make([]byte, len(expected))
		err = fh.ReadAt(p, read)
		if err != nil {
			t.Fatalf("unable to read: %v", err)
		}
		if !bytes.Equal(expected, p) {
			t.Fatalf("expected %v, got %v at offset %d", expected, p, read)
		}
		read += uint64(len(expected))
	}
}

// TestBeeMovie writes the Bee Movie script until the handle is full, and then
// checks it to make sure that it's correct.
func TestBeeMovie(t *testing.T) {
	t.Parallel()

	getBeeMovieScript := func() ([]byte, error) {
		const url = "https://gist.githubusercontent.com/The5heepDev/a15539b297a7862af4f12ce07fee6bb7/raw/7164813a9b8d0a3b2dcffd5b80005f1967887475/entire_bee_movie_script"
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	}
	script, err := getBeeMovieScript()
	if err != nil {
		t.Fatalf("unable to get script: %v", err)
	}

	f, err := NewBackend("repl-cloudblock")
	if err != nil {
		t.Fatalf("unable to create file backend: %v", err)
	}

	f.Delete("test-bee-movie")
	time.Sleep(time.Second) // wait for rate limit to settle

	// make sure we conform to the interface
	var fh cloudblock.Handle

	// 1 megabyte, 50 kilobyte bands
	const size = 1000 * 1000
	fh, err = f.New("test-bee-movie", size, 50*1000)
	if err != nil {
		t.Fatalf("unable to create handle: %v", err)
	}

	written := uint64(0)
	for {
		if written+uint64(len(script)) > size {
			break
		}
		p := make([]byte, len(script))
		copy(p, script)
		err = fh.WriteAt(script, written)
		if err != nil {
			t.Fatalf("unable to write: %v", err)
		}
		written += uint64(len(script))
	}

	// read it back
	read := uint64(0)
	for {
		if read+uint64(len(script)) > size {
			break
		}
		p := make([]byte, len(script))
		err = fh.ReadAt(p, read)
		if err != nil {
			t.Fatalf("unable to read: %v", err)
		}
		if !bytes.Equal(script, p) {
			t.Fatalf("expected %v, got %v at offset %d", script, p, read)
		}
		read += uint64(len(script))
	}
}
