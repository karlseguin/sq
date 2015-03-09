package garbage4

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"syscall"
	"unsafe"
)

var (
	encoder           = binary.LittleEndian
	blank             = struct{}{}
	ChannelNameLenErr = fmt.Errorf("channel name cannot exceed %d characters", MAX_CHANNEL_NAME_SIZE)
	ChannelExistsErr  = fmt.Errorf("channel already exists")
	ChannelCreateErr  = fmt.Errorf("channel count not be created")
)

type addChannelWork struct {
	err     error
	name    string
	channel *Channel
	c       chan *addChannelWork
}

type Topic struct {
	sync.RWMutex
	dataLock     sync.RWMutex
	path         string
	state        *State
	states       *States
	channels     map[string]*Channel
	segment      *Segment
	segments     map[uint64]*Segment
	addChannel   chan *addChannelWork
	messageAdded chan struct{}
	// segmentDone  chan uint64
	pageSize int
}

func OpenTopic(name string, config *Configuration) (*Topic, error) {
	t := &Topic{
		path:       path.Join(config.path, name),
		channels:   make(map[string]*Channel),
		segments:   make(map[uint64]*Segment),
		addChannel: make(chan *addChannelWork),
		// segmentDone:  make(chan uint64, 8),
		messageAdded: make(chan struct{}, 64),
		pageSize:     os.Getpagesize(),
	}
	err := loadStates(t)
	if err != nil {
		return nil, err
	}

	if id := t.state.segmentId; id == 0 {
		t.expand()
	} else {
		t.segment = openSegment(t, id, false)
		t.segments[id] = t.segment
	}
	go t.worker()
	return t, nil
}

func (t *Topic) Write(data []byte) error {
	length := len(data)

	t.dataLock.Lock()
	start := int(t.state.offset)
	dataStart := start + 4
	dataEnd := dataStart + length

	// do we have enough space in the current segment?
	if dataEnd > MAX_SEGMENT_SIZE {
		t.expand()
		start = int(SEGMENT_HEADER_SIZE)
		dataStart = start + 4
		dataEnd = dataStart + length
	}

	// write the message length
	encoder.PutUint32(t.segment.data[start:], uint32(length))
	// write the message
	copy(t.segment.data[dataStart:], data)
	// location of the next write
	t.state.offset = uint32(dataEnd)
	// update the size of this segment
	t.segment.size += uint32(length + 4)
	t.dataLock.Unlock()

	// sync the part of the data file we just wrote
	from := start / t.pageSize * t.pageSize
	to := dataStart + length - from
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&t.segment.data[from])), uintptr(to), syscall.MS_SYNC)
	if errno != 0 {
		return syscall.Errno(errno)
	}

	// notify channels that a new message is waiting
	t.messageAdded <- blank

	return nil
}

func (t *Topic) Channel(name string) (*Channel, error) {
	if len(name) > MAX_CHANNEL_NAME_SIZE {
		return nil, ChannelNameLenErr
	}
	res := &addChannelWork{
		name: name,
		c:    make(chan *addChannelWork),
	}
	t.addChannel <- res
	<-res.c
	return res.channel, res.err
}

func (t *Topic) expand() {
	segment := newSegment(t)
	t.segments[segment.id] = segment
	if t.segment != nil {
		t.segment.nextId = segment.id
	}
	t.segment = segment
	t.state.offset = SEGMENT_HEADER_SIZE
	t.state.segmentId = segment.id
}

func (t *Topic) worker() {
	for {
		select {
		case work := <-t.addChannel:
			work.channel, work.err = t.createChannel(work.name)
			work.c <- work
		case <-t.messageAdded:
			t.RLock()
			for _, c := range t.channels {
				c.notify(1)
			}
			t.RUnlock()
			// case id := <-t.segmentDone:
			// 	t.dataLock.RLock()
			// 	usable := t.state.usable(id)
			// 	t.dataLock.RUnlock()
			// 	if usable == false {
			// 		t.Lock()
			// 		segment := t.segments[id]
			// 		delete(t.segments, id)
			// 		t.Unlock()
			// 		if segment != nil {
			// 			segment.delete() //too channels can finish with a segment at the same time
			// 		}
			// 	}
		}
	}
}

func (t *Topic) createChannel(name string) (*Channel, error) {
	temp := false
	if len(name) == 0 {
		name = "tmp." + strconv.Itoa(rand.Int())
		temp = true
	}
	t.Lock()
	_, exists := t.channels[name]
	if exists {
		t.Unlock()
		if temp {
			// try again until we've created one
			return t.createChannel("")
		}
		return nil, ChannelExistsErr
	}
	defer t.Unlock()

	c := newChannel(t, name)
	if temp {
		c.state = new(State)
	} else {
		c.state = t.states.getOrCreate(name)
		if c.state == nil {
			return nil, ChannelCreateErr
		}
	}

	// if we have a temp channel, or a new channel, set the position to the
	// writer's current position
	if temp || c.state.segmentId == 0 {
		t.dataLock.RLock()
		c.state.offset = t.state.offset
		c.state.segmentId = t.state.segmentId
		t.dataLock.RUnlock()
	}
	t.channels[name] = c
	return c, nil
}

func (t *Topic) align(c *Channel) bool {
	t.dataLock.Lock()
	defer t.dataLock.Unlock()
	if t.lockedRead(c.state) != nil {
		return false
	}
	count := 0
	for {
		select {
		case <-t.messageAdded:
			count++
		default:
			t.Lock()
			for _, c := range t.channels {
				c.notify(count)
			}
			t.Unlock()
			c.aligned()
			return true
		}
	}
}

func (t *Topic) read(state *State) []byte {
	t.dataLock.RLock()
	defer t.dataLock.RUnlock()
	return t.lockedRead(state)
}

func (t *Topic) lockedRead(state *State) []byte {
	segment := t.segment
	if state.segmentId == segment.id {
		if state.offset >= t.state.offset {
			return nil
		}
	} else {
		segment = t.loadSegment(state.segmentId)
		if state.offset >= segment.size {
			// previousId := segment.id
			segment = t.loadSegment(segment.nextId)
			state.segmentId = segment.id
			state.offset = SEGMENT_HEADER_SIZE
			// t.segmentDone <- previousId
		}
	}

	l := encoder.Uint32(segment.data[state.offset:])
	start := state.offset + 4
	end := start + uint32(l)
	return segment.data[start:end]
}

func (t *Topic) loadSegment(id uint64) *Segment {
	t.RLock()
	segment := t.segments[id]
	t.RUnlock()
	if segment != nil {
		return segment
	}
	t.Lock()
	defer t.Unlock()
	segment = t.segments[id]
	if segment != nil {
		return segment
	}
	segment = openSegment(t, id, false)
	t.segments[id] = segment
	return segment
}
