package garbage4

import (
	"encoding/binary"
	"log"
	"os"
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

type Topic struct {
	sync.RWMutex
	dataLock     sync.RWMutex
	name         string
	state        *State
	channels     map[string]*Channel
	position     *Position
	segment      *Segment
	segments     map[uint64]*Segment
	addChannel   chan string
	channelAdded chan *Channel
	messageAdded chan uint32
	segmentDone  chan uint64
	pageSize     int
}

func OpenTopic(name string) (*Topic, error) {
	t := &Topic{
		name:         name,
		channels:     make(map[string]*Channel),
		segments:     make(map[uint64]*Segment),
		addChannel:   make(chan string),
		channelAdded: make(chan *Channel),
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

func (t *Topic) Channel(name string) *Channel {
	t.addChannel <- name
	return <-t.channelAdded
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
		case name := <-t.addChannel:
			t.RLock()
			c := t.channels[name]
			t.RUnlock()
			if c != nil {
				log.Printf("multiple instances of channel %s on topic %s\n", name, t.name)
			} else {
				c = newChannel(t, name)
				c.position = t.state.loadOrCreatePosition(name)
				if c.position.segmentId == 0 {
					t.dataLock.RLock()
					c.position.offset = t.position.offset
					c.position.segmentId = t.position.segmentId
					t.dataLock.RUnlock()
				}
				t.Lock()
				t.channels[name] = c
				t.Unlock()
			}
			t.channelAdded <- c
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
