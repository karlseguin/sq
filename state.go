package garbage4

import (
	"os"
	"path"
	"sync"
	"syscall"
	"unsafe"
)

const (
	MAX_CHHANNELS         = 128
	MAX_CHANNEL_NAME_SIZE = 32
)

type States struct {
	sync.RWMutex
	data     []byte
	file     *os.File
	free     []*State
	channels map[string]*State
}

type State struct {
	flag      uint64
	l         byte
	name      [MAX_CHANNEL_NAME_SIZE]rune
	offset    uint32
	segmentId uint64
}

func loadStates(topic *Topic) error {
	if err := os.MkdirAll(topic.path, 0700); err != nil {
		return err
	}
	path := path.Join(topic.path, "state")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	sizePerEntry := int(unsafe.Sizeof(State{}))
	maxSize := sizePerEntry * (MAX_CHHANNELS + 1)
	file.Truncate(int64(maxSize))

	data, err := syscall.Mmap(int(file.Fd()), 0, maxSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return err
	}

	states := &States{
		data:     data,
		file:     file,
		channels: make(map[string]*State),
		free:     make([]*State, 0, MAX_CHHANNELS),
	}

	// // the first entry is always the topic's entry
	topic.state = (*State)(unsafe.Pointer(&states.data[0]))
	for position := sizePerEntry; position < maxSize; position += sizePerEntry {
		state := (*State)(unsafe.Pointer(&states.data[position]))
		if state.l > 0 {
			states.channels[string(state.name[:state.l])] = state
		} else {
			states.free = append(states.free, state)
		}
	}
	topic.states = states
	return nil
}

// func (s *State) loadEntry(offset int) *Position {
// 	return
// }

// synchronization is provided by the topic
func (s *States) getOrCreate(name string) *State {
	if state, exists := s.channels[name]; exists {
		return state
	}

	// no more free space
	// todo: expand the state and remove the limit on channels
	if len(s.free) == 0 {
		return nil
	}
	state := s.free[0]
	state.l = byte(len(name))
	for i, r := range name {
		state.name[i] = r
	}
	s.free = s.free[1:]
	return state
}

func (s *States) usable(segmentId uint64) bool {
	// for _, entry := range s.entries {
	// if s.loadPosition(offset).segmentId <= segmentId {
	// 	return true
	// }
	// }
	return false
}
