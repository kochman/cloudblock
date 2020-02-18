// Package nbd is a Network Block Device server implemented based on
// https://github.com/NetworkBlockDevice/nbd/blob/cb20c16354cccf4698fde74c42f5fb8542b289ae/doc/proto.md
package nbd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/kochman/cloudblock"
	"github.com/kochman/cloudblock/backends/file"
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
		go handle(newConnection(conn))
	}
}

type connection struct {
	nc net.Conn
	b  *bufio.ReadWriter
}

func newConnection(nc net.Conn) *connection {
	c := &connection{
		nc: nc,
		b:  bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
	}
	return c
}

type export struct {
	name string
	h    cloudblock.Handle
}

func newExport(name string) (*export, error) {
	const dir = "cloudblock-backend-file"
	err := os.Mkdir(dir, 0755)
	if err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("unable to create dir: %v", err)
	}

	f, err := file.NewBackend(dir)
	if err != nil {
		return nil, fmt.Errorf("unable to create file backend: %v", err)
	}

	// 1 gigabyte, 4 MB bands
	fh, err := f.New(name, 1000*1000*1000, 4*1000*1000)
	if err == cloudblock.HandleExists {
		fh, err = f.Open(name)
		if err != nil {
			return nil, fmt.Errorf("unable to open handle: %v", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("unable to create handle: %v", err)
	}

	e := &export{
		name: name,
		h:    fh,
	}
	return e, nil
}

func handle(c *connection) {
	log.Printf("handling %s", c.nc.RemoteAddr())
	defer c.Close()

	// write some magic numbers
	err := c.WriteUint64(0x4e42444d41474943)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
	err = c.WriteUint64(0x49484156454F5054)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}

	// handshake flags
	err = c.WriteUint16(0x8000)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}

	err = c.Flush()
	if err != nil {
		log.Printf("error: %v", err)
		return
	}

	p := make([]byte, 4)
	c.nc.Read(p)
	clientFlags := binary.BigEndian.Uint32(p)
	if clientFlags != 1 {
		log.Printf("closing conn due to unsupported flags")
		c.nc.Close()
		return
	}

	// soak up all the client's opts
	for {
		p = make([]byte, 8)
		c.nc.Read(p)
		magic := binary.BigEndian.Uint64(p)
		if magic != 0x49484156454F5054 {
			break
		}

		// read option
		opt, err := c.ReadUint32()
		if err != nil {
			log.Printf("error: %v", err)
			return
		}

		// read length
		l, err := c.ReadUint32()
		if err != nil {
			log.Printf("error: %v", err)
			return
		}

		// read data
		data := make([]byte, l)
		err = c.ReadFull(data)
		if err != nil {
			log.Printf("unable to read data: %v", err)
			return
		}
		// log.Printf("got opt [%v] length [%d] data [%v]", opt, l, data)

		switch opt {
		case 7: // NBD_OPT_GO
			p = make([]byte, 4)
			l := binary.BigEndian.Uint32(data[:4])
			name := data[4 : 4+l]
			// log.Printf("export name: %s", name)

			e, err := newExport(string(name))
			if err != nil {
				log.Printf("bad export [%s]: %v", name, err)
				return
			}

			// get info requests
			offset := 4 + l
			numReqs := binary.BigEndian.Uint16(data[offset : offset+2])
			if numReqs > 0 {
				panic("uh oh we don't support that yet")
			}
			// log.Printf("num info requests: %d", numReqs)
			// offset += 2
			// for i := uint16(0); i < numReqs; i++ {
			// 	offset = offset + uint32(i)*2
			// 	log.Printf("info req: %s", data[offset:offset+2])
			// }

			// reply with some info about the export (NBD_REP_INFO and NBD_INFO_EXPORT)
			err = c.WriteUint64(0x3e889045565a9)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			err = c.WriteUint32(opt)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			// reply type
			err = c.WriteUint32(3)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			// length
			err = c.WriteUint32(12)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			err = c.WriteUint16(0)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			// export size
			size, err := e.h.Size()
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			err = c.WriteUint64(size)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			// transmission flags
			err = c.WriteUint16(0x8000)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}

			// we're done giving out info, send a NBD_REP_ACK
			err = c.WriteUint64(0x3e889045565a9)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			err = c.WriteUint32(opt)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			// reply type
			err = c.WriteUint32(1)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}
			err = c.WriteUint32(0)
			if err != nil {
				log.Printf("error: %v", err)
				return
			}

			err = c.Flush()
			if err != nil {
				log.Printf("flush error: %v", err)
				return
			}

			// if we got here then we're in transmission phase
			handleTransmission(c, e)

		default:
			p = make([]byte, 8)
			binary.BigEndian.PutUint64(p, 2^31+1)
			c.nc.Write(p)
		}
	}

	log.Printf("done")
}

func handleTransmission(c *connection, e *export) {
	log.Printf("handling transmission for export [%s]", e.name)

	for {
		// requests start with NBD_REQUEST_MAGIC
		magic, err := c.ReadUint32()
		if err != nil {
			log.Printf("error: %v", err)
			return
		}

		if magic != 0x25609513 {
			log.Printf("unknown magic num")
			return
		}

		// handle request
		cmdFlags, err := c.ReadUint16()
		if err != nil {
			log.Printf("error: %v", err)
			return
		}
		_ = cmdFlags
		typ, err := c.ReadUint16() // 0 is read, 1 is write, 2 is disconnect, 3 is flush...
		if err != nil {
			log.Printf("error: %v", err)
			return
		}
		handle, err := c.ReadUint64()
		if err != nil {
			log.Printf("error: %v", err)
			return
		}
		_ = handle
		offset, err := c.ReadUint64()
		if err != nil {
			log.Printf("error: %v", err)
		}
		length, err := c.ReadUint32()
		if err != nil {
			log.Printf("error: %v", err)
		}

		switch typ {
		case 0: // read
			log.Printf("read request: offset %d length %d", offset, length)
			handleRead(c, e.h, handle, offset, length)
		case 1:
			log.Printf("write request: offset %d length %d", offset, length)
			handleWrite(c, e.h, handle, offset, length)
		case 2:
			log.Printf("disconnect request")
			return
		default:
			log.Printf("unhandled request type %d", typ)
			return
		}
	}
}

func handleRead(c *connection, h cloudblock.Handle, handle, offset uint64, length uint32) {
	p := make([]byte, length)
	err := h.ReadAt(p, offset)
	if err != nil {
		log.Printf("error handling read: %v", err)
		return
	}

	// NBD_SIMPLE_REPLY_MAGIC
	err = c.WriteUint32(0x67446698)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
	err = c.WriteUint32(0)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
	err = c.WriteUint64(handle)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
	err = c.Write(p)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
	err = c.Flush()
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
}

func handleWrite(c *connection, h cloudblock.Handle, handle, offset uint64, length uint32) {
	p := make([]byte, length)
	err := c.ReadFull(p)
	if err != nil {
		log.Printf("error handling write: %v", err)
		return
	}
	err = h.WriteAt(p, offset)
	if err != nil {
		log.Printf("error handling write: %v", err)
		return
	}

	// NBD_SIMPLE_REPLY_MAGIC
	err = c.WriteUint32(0x67446698)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
	err = c.WriteUint32(0)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
	err = c.WriteUint64(handle)
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
	err = c.Flush()
	if err != nil {
		log.Printf("error: %v", err)
		return
	}
}

func (c *connection) ReadFull(p []byte) error {
	_, err := io.ReadFull(c.b, p)
	return err
	// lr := io.LimitReader(c.nc, int64(len(p)))
	// for i := 0; i < len(p); i++ {
	// 	thisRead, err := lr.Read(p[i:])
	// 	if err != nil {
	// 		return err
	// 	}
	// 	i += thisRead
	// }
	// return nil
}

func (c *connection) ReadUint16() (uint16, error) {
	p := make([]byte, 2)
	_, err := io.ReadFull(c.b, p)
	return binary.BigEndian.Uint16(p), err
}

func (c *connection) ReadUint32() (uint32, error) {
	p := make([]byte, 4)
	_, err := io.ReadFull(c.b, p)
	return binary.BigEndian.Uint32(p), err
}

func (c *connection) ReadUint64() (uint64, error) {
	p := make([]byte, 8)
	_, err := io.ReadFull(c.b, p)
	return binary.BigEndian.Uint64(p), err
}

func (c *connection) WriteUint16(data uint16) error {
	p := make([]byte, 2)
	binary.BigEndian.PutUint16(p, data)
	_, err := c.b.Write(p)
	return err
}

func (c *connection) WriteUint32(data uint32) error {
	p := make([]byte, 4)
	binary.BigEndian.PutUint32(p, data)
	_, err := c.b.Write(p)
	return err
}

func (c *connection) WriteUint64(data uint64) error {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, data)
	_, err := c.b.Write(p)
	return err
}

func (c *connection) Write(b []byte) error {
	_, err := c.b.Write(b)
	return err
}

func (c *connection) Flush() error {
	return c.b.Flush()
}

func (c *connection) Close() {
	c.b.Flush()
	c.nc.Close()
}
