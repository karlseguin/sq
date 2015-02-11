package garbage4

import (
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const MAX_STATE_SIZE = 32768

type State struct {
	sync.RWMutex
	ref      []byte
	file     *os.File
	offset   int
	data     *[MAX_STATE_SIZE]byte
	channels map[string]int
}

type Position struct {
	offset    uint32
	segmentId uint64
}

func loadState(t *Topic) (*State, error) {
	root := PATH + t.name
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	path := root + "/state.q"
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if size := info.Size(); size == 0 {
		file.Truncate(MAX_STATE_SIZE)
	} else if size != MAX_STATE_SIZE {
		panic("invalid state file size")
	}

	ref, err := syscall.Mmap(int(file.Fd()), 0, MAX_STATE_SIZE, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}

	state := &State{
		ref:      ref,
		file:     file,
		channels: make(map[string]int),
		data:     (*[MAX_STATE_SIZE]byte)(unsafe.Pointer(&ref[0])),
	}

	start, end := 16, 16
	for {
		if state.data[end] == 0 {
			if end == start {
				break
			}
			offset := end + 1
			state.channels[string(state.data[start:end])] = offset
			start = offset + 16
			end = start
		} else {
			end++
		}
	}
	state.offset = end
	return state, nil
}

func (s *State) loadPosition(offset int) *Position {
	return (*Position)(unsafe.Pointer(&s.data[offset]))
}

func (s *State) loadOrCreatePosition(name string) *Position {
	offset, exists := s.channels[name]
	if exists == false {
		//todo check for overflow
		//todo compact
		s.offset += copy(s.data[s.offset:], name)
		s.data[s.offset] = 0
		offset = s.offset + 1
		s.offset += 17 //segmentId + offset + 1 for name null
		s.channels[name] = offset
	}
	return s.loadPosition(offset)
}

func (s *State) usable(segmentId uint64) bool {
	for _, offset := range s.channels {
		if s.loadPosition(offset).segmentId <= segmentId {
			return true
		}
	}
	return false
}
