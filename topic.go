package garbage4

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"syscall"
	"unsafe"
)

const (
	MAX_QUEUE_SIZE = 256
)

var (
	PATH    = "/tmp/q/"
	encoder = binary.LittleEndian
	blank   = struct{}{}
)

func init() {
	if err := os.MkdirAll(PATH, 0700); err != nil {
		panic(err)
	}
}

type addChannelWork struct {
	err     error
	name    string
	channel *Channel
	c    chan *addChannelWork
}

type Topic struct {
	sync.RWMutex
	dataLock     sync.RWMutex
	name         string
	state        *State
	channels     map[string]*Channel
	position     *Position
	segment      *Segment
	segments     map[uint64]*Segment
	addChannel   chan *addChannelWork
	messageAdded chan uint32
	segmentDone  chan uint64
	pageSize     int
}

func OpenTopic(name string) (*Topic, error) {
	t := &Topic{
		name:         name,
		channels:     make(map[string]*Channel),
		segments:     make(map[uint64]*Segment),
		addChannel:   make(chan *addChannelWork),
		segmentDone:  make(chan uint64, 8),
		messageAdded: make(chan uint32, 64),
		pageSize:     os.Getpagesize(),
	}
	state, err := loadState(t)
	if err != nil {
		return nil, err
	}

	t.state = state
	t.position = state.loadPosition(0)
	if id := t.position.segmentId; id == 0 {
		t.expand()
	} else {
		t.segment = openSegment(t, id, false)
		t.segments[id] = t.segment
	}
	go t.worker()
	return t, nil
}

func (t *Topic) Write(data []byte) error {
	l := len(data)
	t.dataLock.Lock()
	start := int(t.position.offset)
	start4 := start + 4
	if start4+l > MAX_QUEUE_SIZE {
		t.expand()
		start = SEGMENT_HEADER_SIZE
		start4 = start + 4
	}

	//write
	encoder.PutUint32(t.segment.data[start:], uint32(l))
	copy(t.segment.data[start4:], data)
	t.position.offset = uint32(start4 + l)
	l4 := uint32(l + 4)
	t.segment.size += l4
	t.dataLock.Unlock()
	t.messageAdded <- l4

	//msync
	from := start / t.pageSize * t.pageSize
	to := start4 + l - from
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&t.segment.data[from])), uintptr(to), syscall.MS_SYNC)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func (t *Topic) Channel(name string) (*Channel, error) {
	res := &addChannelWork{
		name: name,
		c: make(chan *addChannelWork),
	}
	t.addChannel <- res
	<- res.c
	return res.channel, res.err
}

func (t *Topic) expand() {
	segment := newSegment(t)
	t.segments[segment.id] = segment
	if t.segment != nil {
		t.segment.nextId = segment.id
	}
	t.segment = segment
	t.position.offset = SEGMENT_HEADER_SIZE
	t.position.segmentId = segment.id
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
		case id := <-t.segmentDone:
			t.dataLock.RLock()
			usable := t.state.usable(id)
			t.dataLock.RUnlock()
			if usable == false {
				t.Lock()
				segment := t.segments[id]
				delete(t.segments, id)
				t.Unlock()
				if segment != nil {
					segment.delete() //too channels can finish with a segment at the same time
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
	t.Lock()
	defer t.Unlock()
	if _, exists := t.channels[name]; exists {
		if temp {
			return t.createChannel("")
		}
		return nil, fmt.Errorf("channel %q for topic %q already exists", name, t.name)
	}

	c := newChannel(t, name)
	if temp {
		c.position = new(Position)
	} else {
		c.position = t.state.loadOrCreatePosition(name)
	}
	if temp || c.position.segmentId == 0 {
		t.dataLock.RLock()
		c.position.offset = t.position.offset
		c.position.segmentId = t.position.segmentId
		t.dataLock.RUnlock()
	}
	t.channels[name] = c
	return c, nil
}

func (t *Topic) align(c *Channel) bool {
	t.dataLock.Lock()
	defer t.dataLock.Unlock()
	if t.lockedRead(c.position) != nil {
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

func (t *Topic) read(position *Position) []byte {
	t.dataLock.RLock()
	defer t.dataLock.RUnlock()
	return t.lockedRead(position)
}

func (t *Topic) lockedRead(position *Position) []byte {
	segment := t.segment
	if position.segmentId == segment.id {
		if position.offset >= t.position.offset {
			return nil
		}
	} else {
		segment = t.loadSegment(position.segmentId)
		if position.offset >= segment.size {
			previousId := segment.id
			segment = t.loadSegment(segment.nextId)
			position.segmentId = segment.id
			position.offset = SEGMENT_HEADER_SIZE
			t.segmentDone <- previousId
		}
	}

	l := encoder.Uint32(segment.data[position.offset:])
	start := position.offset + 4
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
