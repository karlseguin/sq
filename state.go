package garbage4

import (
	"os"
	"path"
	"sync"
	"syscall"
	"unsafe"
)

const (
	POSITION_SIZE  = 16
	MAX_STATE_SIZE = 32768
)

var (
	MAX_CHANNEL_NAME_SIZE = 32
)

type State struct {
	sync.RWMutex
	ref      []byte
	file     *os.File
	data     *[MAX_STATE_SIZE]byte
	channels map[string]int
}

type Position struct {
	offset    uint32
	segmentId uint64
}

func loadState(t *Topic) (*State, error) {
	root := t.path
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	path := path.Join(root, "state.q")
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

	offset := POSITION_SIZE
	recordSize := offset + MAX_CHANNEL_NAME_SIZE
	for ; (offset+recordSize) < MAX_STATE_SIZE && state.data[offset] != 0; offset += recordSize {
		//deleted channel
		if state.data[offset] == 255 {
			continue
		}
		end := offset
		for ; (end-offset) < MAX_CHANNEL_NAME_SIZE && state.data[end] != 0; end++ {
		}
		println(offset, end, string(state.data[offset:end]))
		state.channels[string(state.data[offset:end])] = offset + MAX_CHANNEL_NAME_SIZE
	}
	return state, nil
}

func (s *State) loadPosition(offset int) *Position {
	return (*Position)(unsafe.Pointer(&s.data[offset]))
}

func (s *State) loadOrCreatePosition(name string) *Position {
	offset, exists := s.channels[name]
	if exists == false {
		//todo check for overflow
		offset = POSITION_SIZE
		for ; offset < MAX_CHANNEL_NAME_SIZE; offset += MAX_CHANNEL_NAME_SIZE + POSITION_SIZE {
			b := s.data[offset]
			if b == 0 || b == 255 {
				break
			}
		}
		copy(s.data[offset:], name)
		offset += MAX_CHANNEL_NAME_SIZE
		s.channels[name] = offset
	}
	println(offset)
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
