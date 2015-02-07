package garbage4

import (
	"os"
	"strconv"
	"syscall"
	"time"
	"unsafe"
)

type Segment struct {
	id   uint64
	ref  []byte
	file *os.File
	data *[MAX_QUEUE_SIZE]byte
}

func newSegment(t *Topic) *Segment {
	segment := openSegment(t, uint64(time.Now().UnixNano()))
	segment.file.Truncate(MAX_QUEUE_SIZE)
	return segment
}

func openSegment(t *Topic, id uint64) *Segment {
	name := PATH + t.name + "/" + strconv.FormatUint(id, 10) + ".q"
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	ref, err := syscall.Mmap(int(file.Fd()), 0, MAX_QUEUE_SIZE, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	return &Segment{
		id:   id,
		ref:  ref,
		file: file,
		data: (*[MAX_QUEUE_SIZE]byte)(unsafe.Pointer(&ref[0])),
	}
}

func (s *Segment) Close() {
	syscall.Munmap(s.ref)
	s.file.Close()
	s.data, s.ref = nil, nil
}
