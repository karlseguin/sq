package sqd

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
)

var (
	encoder         = binary.LittleEndian
	LargeMessageErr = errors.New("Maximum message size exceeded")
)

type Reader struct {
	net.Conn
	buffer   []byte
	message  []byte
	position int
}

func NewReader(conn net.Conn, bufferSize int) *Reader {
	if bufferSize < 1024 {
		bufferSize = 1024
	}
	return &Reader{
		Conn:   conn,
		buffer: make([]byte, bufferSize),
	}
}

func (r *Reader) ReadMessage() (Message, error) {
	if _, err := io.ReadFull(r, r.buffer[:4]); err != nil {
		return nil, err
	}
	l := encoder.Uint32(r.buffer)
	if l > 1048576 {
		return nil, LargeMessageErr
	}
	if l > uint32(cap(r.buffer)) {
		r.message = make([]byte, l)
	} else {
		r.message = r.buffer[:l]
	}
	if _, err := io.ReadFull(r, r.message); err != nil {
		return nil, err
	}
	r.position = 0
	return MessageFactory(r)
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
