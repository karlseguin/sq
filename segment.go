package sq

import (
	"os"
	"path"
	"strconv"
	"syscall"
	"time"
	"unsafe"
)

const (
	MAX_SEGMENT_SIZE = 16777216
)

var (
	SEGMENT_HEADER_SIZE = uint32(unsafe.Sizeof(Header{}))
)

type Segment struct {
	*Header
	ref  []byte
	file *os.File
	data *[MAX_SEGMENT_SIZE]byte
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
	segment.size = SEGMENT_HEADER_SIZE
	return segment
}

func openSegment(t *Topic, id uint64, isNew bool) *Segment {
	name := path.Join(t.path, strconv.FormatUint(id, 10)+".q")
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	if isNew {
		file.Truncate(MAX_SEGMENT_SIZE)
	}
	ref, err := syscall.Mmap(int(file.Fd()), 0, MAX_SEGMENT_SIZE, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	s := &Segment{
		ref:  ref,
		file: file,
		data: (*[MAX_SEGMENT_SIZE]byte)(unsafe.Pointer(&ref[0])),
	}

	s.Header = (*Header)(unsafe.Pointer(&s.data[0]))
	// The size of a segment recorded in the header isn't guaranteed to be correct.
	// We msync the data portion, not the head. Since the data portion is correct
	// we can figure out the correct (we start from the recorded size since we
	// know the real size is this or larger)
	if isNew == false {
		offset := s.size
		for {
			offset4 := offset + 4
			if offset4 >= MAX_SEGMENT_SIZE {
				break
			}
			l := encoder.Uint32(s.data[offset:])
			if l == 0 {
				break
			}
			offset = offset4 + l
		}
		s.size = offset
	}
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
	syscall.Munmap(s.ref)
	s.file.Close()
	s.data, s.ref = nil, nil
	os.Remove(s.file.Name())
}
