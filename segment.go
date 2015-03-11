package sq

import (
	"os"
	"path"
	"strconv"
	"syscall"
	"time"
	"unsafe"
)

var (
	SEGMENT_HEADER_SIZE = uint32(unsafe.Sizeof(Header{}))
)

type Segment struct {
	*Header
	data []byte
	file *os.File
}

type Header struct {
	version uint32
	flag    uint32
	size    uint32
	id      uint64
	nextId  uint64
}

func newSegment(t *Topic) *Segment {
	id := uint64(time.Now().UnixNano())
	segment := openSegment(t, id, true)
	segment.id = id
	segment.syncHeader()
	return segment
}

func openSegment(t *Topic, id uint64, isNew bool) *Segment {
	name := path.Join(t.path, strconv.FormatUint(id, 10)+".q")
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	if isNew {
		file.Truncate(int64(t.segmentSize))
	}
	data, err := syscall.Mmap(int(file.Fd()), 0, t.segmentSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	s := &Segment{
		file: file,
		data: data,
	}
	s.Header = (*Header)(unsafe.Pointer(&s.data[0]))
	return s
}

func (s *Segment) syncHeader() error {
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&s.data[0])), uintptr(pageSize), syscall.MS_SYNC)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func (s *Segment) delete() {
	syscall.Munmap(s.data)
	s.file.Close()
	s.data = nil
	os.Remove(s.file.Name())
}
