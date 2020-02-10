package main

import (
	"log"
	"os"
	"time"

	"sidney.kochman.org/cloudblock"
	"sidney.kochman.org/cloudblock/backends/gcs"
)

const megabyte = 1000 * 1000

func main() {
	f, err := gcs.NewBackend("cloudblock-test")
	if err != nil {
		log.Printf("unable to create gcs backend: %v", err)
		os.Exit(1)
	}
	var fh cloudblock.Handle
	fh, err = f.New("test", 1000*megabyte, 4*megabyte)
	if err != nil {
		log.Printf("unable to create \"test\": %v", err)
		os.Exit(1)
	}
	// fh, err = f.Open("test")
	// if err != nil {
	// 	log.Printf("unable to open \"test\": %v", err)
	// 	os.Exit(1)
	// }
	// _ = fh

	alpha := []byte{}
	for i := 0; i < megabyte; i++ {
		alpha = append(alpha, byte(i))
	}
	start := time.Now()
	for i := 0; i < 1024*1024*1024; i += len(alpha) {
		err = fh.WriteAt(alpha, uint64(i))
		if err != nil {
			log.Printf("unable to write: %v", err)
			os.Exit(1)
		}
	}
	log.Println(time.Since(start))

	// g, err := gcs.NewBackend()
	// if err != nil {
	// 	log.Printf("unable to create GCS backend: %v", err)
	// 	os.Exit(1)
	// }
	// gh, err := g.Open("test")
	// if err != nil {
	// 	log.Printf("unable to open \"test\": %v", err)
	// 	os.Exit(1)
	// }
	// _ = gh
}
