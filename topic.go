package sq

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
	pageSize          = os.Getpagesize()
)

type addChannelWork struct {
	err     error
	name    string
	channel *Channel
	c       chan *addChannelWork
}

type Topic struct {
	path         string
	state        *State
	states       *States
	channelsLock sync.RWMutex
	channels     map[string]*Channel
	segment      *Segment
	segmentsLock sync.RWMutex
	segments     map[uint64]*Segment
	addChannel   chan *addChannelWork
	messageAdded chan struct{}
	segmentDone  chan uint64
	dataLock     sync.RWMutex
}

func OpenTopic(name string, config *TopicConfiguration) (*Topic, error) {
	t := &Topic{
		path:         path.Join(config.path, name),
		channels:     make(map[string]*Channel),
		segments:     make(map[uint64]*Segment),
		addChannel:   make(chan *addChannelWork),
		segmentDone:  make(chan uint64, 8),
		messageAdded: make(chan struct{}, 64),
	}
	err := loadStates(t)
	if err != nil {
		return nil, err
	}

	if id := t.state.segmentId; id == 0 {
		if err := t.expand(); err != nil {
			return nil, err
		}
		if err := t.states.syncTopic(); err != nil {
			return nil, err
		}
	} else {
		t.findWritePosition(openSegment(t, id, false))
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
		if err := t.expand(); err != nil {
			return err
		}
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
	from := start / pageSize * pageSize
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

func (t *Topic) expand() error {
	segment := newSegment(t)
	t.segmentsLock.Lock()
	t.segments[segment.id] = segment
	t.segmentsLock.Unlock()
	if t.segment != nil {
		// create a pointer to the next segment id
		t.segment.nextId = segment.id
		if err := t.segment.syncHeader(); err != nil {
			return err
		}
		// it's possible this segment can be cleaned up (if we have no channels)
		t.segmentDone <- t.segment.id
	}
	t.segment = segment
	t.state.offset = SEGMENT_HEADER_SIZE
	t.state.segmentId = segment.id
	return nil
}

func (t *Topic) worker() {
	for {
		select {
		case work := <-t.addChannel:
			work.channel, work.err = t.createChannel(work.name)
			work.c <- work
		case <-t.messageAdded:
			t.channelsLock.RLock()
			for _, c := range t.channels {
				c.notify()
			}
			t.channelsLock.RUnlock()
		case id := <-t.segmentDone:
			t.dataLock.RLock()
			// shortcircuit if this is the topic's segment
			if id == t.segment.id {
				t.dataLock.RUnlock()
				break
			}
			usable := t.isSegmentUsable(id)
			t.dataLock.RUnlock()
			if usable == false {
				t.segmentsLock.Lock()
				segment := t.segments[id]
				delete(t.segments, id)
				t.segmentsLock.Unlock()
				if segment != nil {
					segment.delete()
				}
			}
		}
	}
}

func (t *Topic) createChannel(name string) (*Channel, error) {
	temp := false
	if len(name) == 0 {
		name = "tmp." + strconv.Itoa(rand.Int())
		temp = true
	}
	t.channelsLock.Lock()
	_, exists := t.channels[name]
	if exists {
		t.channelsLock.Unlock()
		if temp {
			// try again until we've created one
			return t.createChannel("")
		}
		return nil, ChannelExistsErr
	}
	defer t.channelsLock.Unlock()

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

func (t *Topic) read(channel *Channel) []byte {
	state := channel.state

	// no need to lock the channel here since the state of the channel is only
	// ever changed after this point (either to move to the next segment or message)
	t.dataLock.RLock()
	isCurrentSegment := state.segmentId == t.state.segmentId
	isCurrentOffset := state.offset == t.state.offset
	t.dataLock.RUnlock()

	// are we fully caught up with the topic?
	if isCurrentSegment && isCurrentOffset {
		return nil
	}

	segment := t.loadSegment(state.segmentId)
	// If we aren't on the current segment, we might be at the end of an old
	// segment. We can safely read old segments without locking since the only
	// writer (the topic), is done with it
	if !isCurrentSegment {
		if state.offset >= segment.size {
			previousId := state.segmentId
			segment = t.loadSegment(segment.nextId)
			channel.Lock()
			state.segmentId = segment.id
			state.offset = SEGMENT_HEADER_SIZE
			channel.Unlock()
			t.segmentDone <- previousId
		}
	}

	l := encoder.Uint32(segment.data[state.offset:])
	start := state.offset + 4
	end := start + uint32(l)
	return segment.data[start:end]
}

func (t *Topic) loadSegment(id uint64) *Segment {
	t.segmentsLock.RLock()
	segment := t.segments[id]
	t.segmentsLock.RUnlock()
	if segment != nil {
		return segment
	}

	t.segmentsLock.Lock()
	defer t.segmentsLock.Unlock()
	if segment := t.segments[id]; segment != nil {
		return segment
	}
	segment = openSegment(t, id, false)
	t.segments[id] = segment
	return segment
}

func (t *Topic) isSegmentUsable(id uint64) bool {
	t.channelsLock.RLock()
	defer t.channelsLock.RUnlock()
	for _, channel := range t.channels {
		if channel.isSegmentUsable(id) == true {
			return true
		}
	}
	return false
}

// The topic's persisted state could be behind from the actual state. However,
// we can determine the actual state based on the data which s guaranteed to be
// up to date. Namely a segment's nextId and its size (note, the persisted size
// also isn't necessarily correct, but openSegment fixes this)
func (t *Topic) findWritePosition(segment *Segment) {
	for segment.nextId != 0 {
		segment = openSegment(t, segment.nextId, false)
	}
	t.segment = segment
	t.state.segmentId = segment.id
	t.state.offset = segment.size
}
