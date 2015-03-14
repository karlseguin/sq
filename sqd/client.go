package sqd

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"
)

var (
	encoder = binary.LittleEndian
)

type Client struct {
	server *Server
	conn   net.Conn
	buffer []byte
	reader *Reader
	writer *Writer
}

func NewClient(conn net.Conn, server *Server) *Client {
	bufferSize := server.config.BufferSize
	if bufferSize < 1024 {
		bufferSize = 1024
	}
	return &Client{
		conn:   conn,
		server: server,
		reader: new(Reader),
		buffer: make([]byte, bufferSize),
		writer: &Writer{original: make([]byte, bufferSize)},
	}
}

func (c *Client) Error(message string, err error) {
	log.Println(message, err)
	c.writer.Type(0)
	c.writer.String(err)
	c.conn.Write(c.writer.End())
	c.conn.Close()
}

func (c *Client) ReadMessage() Message {
	if _, err := io.ReadFull(c.conn, c.buffer[:4]); err != nil {
		c.Error("read message length", err)
		return nil
	}
	l := encoder.Uint32(c.buffer)
	if l > 1048576 { //todo configurable
		c.Error("message too large", nil)
		return nil
	}
	if l > uint32(cap(c.buffer)) {
		c.reader.message = make([]byte, l)
	} else {
		c.reader.message = c.buffer[:l]
	}
	if _, err := io.ReadFull(c.conn, c.reader.message); err != nil {
		c.Error("read message payload", err)
		return nil
	}
	c.reader.position = 0
	message, err := MessageFactory(c.reader)
	if err != nil {
		c.Error("message factory", err)
		return nil
	}
	return message
}

func (c *Client) SetDeadline(t time.Time) {
	c.conn.SetDeadline(t)
}

type Reader struct {
	position int
	message  []byte
}

func (r *Reader) Byte() (b byte) {
	if r.position < len(r.message) {
		b = r.message[r.position]
		r.position++
	}
	return b
}

func (r *Reader) String() (string, error) {
	l, err := r.Length()
	if err != nil {
		return "", err
	}
	b, err := r.ReadN(l)
	return string(b), err
}

func (r *Reader) Strings() ([]string, error) {
	l, err := r.Length()
	if err != nil {
		return nil, err
	}
	strings := make([]string, l)
	for i := 0; i < l; i++ {
		s, err := r.String()
		if err != nil {
			return nil, err
		}
		strings[i] = s
	}
	return strings, nil
}

func (r *Reader) Length() (int, error) {
	b, err := r.ReadN(4)
	if err != nil {
		return 0, err
	}
	return int(encoder.Uint32(b)), nil
}

func (r *Reader) ReadN(n int) ([]byte, error) {
	if r.position+n > len(r.message) {
		return nil, io.EOF
	}
	end := r.position + n
	b := r.message[r.position:end]
	r.position = end
	return b, nil
}

type Writer struct {
	position int
	original []byte
	buffer   []byte
}

func (w *Writer) prepare() {
	// reserve space for the length
	w.buffer = w.original
	w.position = 4
}

func (w *Writer) Type(b byte) {
	w.prepare()
	w.Byte(b)
}

func (w *Writer) Byte(b byte) {
	w.ensure(1)
	w.buffer[w.position] = b
	w.position += 1
}

func (w *Writer) Int(n int) {
	w.ensure(4)
	encoder.PutUint32(w.buffer[w.position:], uint32(n))
	w.position += 4
}

func (w *Writer) String(s string) {
	l := len(s)
	w.ensure(l)
	w.Int(l)
	copy(w.buffer[w.position:], s)
	w.position += l
}

func (w *Writer) End() []byte {
	encoder.PutUint32(w.buffer, uint32(w.position-4))
	return w.buffer[:w.position]
}

func (w *Writer) ensure(n int) {
	if w.position+n < len(w.buffer) {
		return
	}
	buffer := make([]byte, len(w.buffer)*2)
	copy(buffer, w.buffer[:w.position])
	w.buffer = buffer
}
