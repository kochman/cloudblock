// nbd is a Network Block Device server implemented based on
// https://github.com/NetworkBlockDevice/nbd/blob/cb20c16354cccf4698fde74c42f5fb8542b289ae/doc/proto.md
package nbd

import (
	"encoding/binary"
	"log"
	"net"
	"os"
)

func Server() {
	log.Println("hi")

	ln, err := net.Listen("tcp", ":10809")
	if err != nil {
		log.Printf("unable to listen: %v", err)
		os.Exit(1)
	}
	for {
		// TODO: disable nagle
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("unable to accept: %v", err)
			continue
		}
		go handle(conn)
	}
}

func handle(conn net.Conn) {
	log.Printf("handling %s", conn.RemoteAddr())

	// write some magic numbers
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, 0x4e42444d41474943)
	conn.Write(p)
	binary.BigEndian.PutUint64(p, 0x49484156454F5054)
	conn.Write(p)

	// handshake flags
	p = make([]byte, 2)
	binary.BigEndian.PutUint16(p, 0x8000)
	conn.Write(p)

	p = make([]byte, 40)
	conn.Read(p)
	log.Printf("got: %v", p)
}
