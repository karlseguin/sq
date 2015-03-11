package sq

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sync"
	"syscall"
	"unsafe"
)

const (
	MESSAGE_OVERHEAD = 5 //length + termination
)

var (
	encoder            = binary.LittleEndian
	blank              = struct{}{}
	ChannelNameLenErr  = fmt.Errorf("channel name cannot exceed %d characters", MAX_CHANNEL_NAME_SIZE)
	ChannelExistsErr   = fmt.Errorf("channel already exists")
	ChannelCreateErr   = fmt.Errorf("channel count not be created")
	MessageTooLargeErr = fmt.Errorf("message too large")
	pageSize           = os.Getpagesize()
)

type addChannelWork struct {
	err     error
	channel *Channel
	config  *ChannelConfiguration
	c       chan *addChannelWork
}

type Topic struct {
	path         string
	state        *State
	states       *States
	segmentSize  int
	channelsLock sync.RWMutex
	channels     map[string]*Channel
	segment      *Segment
	segmentsLock sync.RWMutex
	segments     map[uint64]*Segment
	addChannel   chan *addChannelWork
	messageAdded chan struct{}
	dataLock     sync.RWMutex
}

func OpenTopic(name string, config *TopicConfiguration) (*Topic, error) {
	t := &Topic{
		path:         path.Join(config.path, name),
		channels:     make(map[string]*Channel),
		segments:     make(map[uint64]*Segment),
		addChannel:   make(chan *addChannelWork),
		segmentSize:  config.segmentSize,
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
	if dataEnd > t.segmentSize {
		if length+MESSAGE_OVERHEAD+int(SEGMENT_HEADER_SIZE) > t.segmentSize {
			return MessageTooLargeErr
		}
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
	t.segment.data[dataEnd] = 255
	// location of the next write
	t.state.offset = uint32(dataEnd) + 1
	t.dataLock.Unlock()

	// sync the part of the data file we just wrote
	from := start / pageSize * pageSize
	to := dataStart + length + 1 - from
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&t.segment.data[from])), uintptr(to), syscall.MS_SYNC)
	if errno != 0 {
		return syscall.Errno(errno)
	}

	// notify channels that a new message is waiting
	t.messageAdded <- blank

	return nil
}

func (t *Topic) Channel(name string, config *ChannelConfiguration) (*Channel, error) {
	if len(name) > MAX_CHANNEL_NAME_SIZE {
		return nil, ChannelNameLenErr
	}
	if config == nil {
		config = ConfigureChannel()
	}
	config.name = name
	res := &addChannelWork{
		c:      make(chan *addChannelWork),
		config: config,
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
		t.segment.size = t.state.offset
		if err := t.segment.syncHeader(); err != nil {
			return err
		}
		t.channelsLock.RLock()
		noChannels := len(t.states.channels) == 0
		t.channelsLock.RUnlock()
		if noChannels {
			t.deleteSegment(t.segment.id)
		}
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
			work.channel, work.err = t.createChannel(work.config)
			work.c <- work
		case <-t.messageAdded:
			t.channelsLock.RLock()
			for _, c := range t.channels {
				c.notify()
			}
			t.channelsLock.RUnlock()
		}
	}
}

func (t *Topic) createChannel(config *ChannelConfiguration) (*Channel, error) {
	t.channelsLock.Lock()
	_, exists := t.channels[config.name]
	if exists {
		t.channelsLock.Unlock()
		if config.temp {
			// try again until we've created one
			return t.createChannel(config)
		}
		return nil, ChannelExistsErr
	}
	defer t.channelsLock.Unlock()

	c := newChannel(t, config)
	if config.temp {
		c.state = new(State)
	} else {
		c.state = t.states.getOrCreate(c.name)
		if c.state == nil {
			return nil, ChannelCreateErr
		}
	}

	// if we have a temp channel, or a new channel, set the position to the
	// writer's current position
	if config.temp || c.state.segmentId == 0 {
		t.dataLock.RLock()
		c.state.offset = t.state.offset
		c.state.segmentId = t.state.segmentId
		t.dataLock.RUnlock()
	}
	t.channels[c.name] = c
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
			channel.changeSegment(segment)
			if t.isSegmentUsable(previousId) == false {
				t.deleteSegment(previousId)
			}
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

// A segment is usable if any channel references it or an earlier one
// How we check this depends on whether or not the channel is active
// (currently consuming message). For an active channel, we ask the channel,
// for an inactive channel, we can query the state direclty knowing that
// no one can be writing to it at the same time
func (t *Topic) isSegmentUsable(id uint64) bool {
	t.channelsLock.RLock()
	defer t.channelsLock.RUnlock()
	for name, state := range t.states.channels {
		if channel, active := t.channels[name]; active {
			if channel.isSegmentUsable(id) == true {
				return true
			}
		} else if state.isSegmentUsable(id) == true {
			return true
		}
	}
	return false
}

// The topic's persisted state could be behind from the actual state. However,
// we can determine the actual state based on the data which is guaranteed to be
// up to date (the next reference id and the data itself)
func (t *Topic) findWritePosition(segment *Segment) {
	for segment.nextId != 0 {
		segment = openSegment(t, segment.nextId, false)
	}
	t.segment = segment
	t.state.segmentId = segment.id

	offset := SEGMENT_HEADER_SIZE
	for offset < uint32(t.segmentSize) {
		l := encoder.Uint32(segment.data[offset:])
		if l == 0 {
			break
		}
		if segment.data[offset+l+4] != 255 {
			break
		}
		offset += l + MESSAGE_OVERHEAD
	}
	t.state.offset = offset
}

func (t *Topic) deleteSegment(id uint64) {
	t.segmentsLock.Lock()
	segment := t.segments[id]
	delete(t.segments, id)
	t.segmentsLock.Unlock()
	if segment != nil {
		segment.delete()
	}
}
