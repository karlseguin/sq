package garbage4

import (
	"encoding/binary"
	"log"
	"os"
	"sync"
	"time"
)

const (
	MAX_QUEUE_SIZE = 16384
)

var (
	PATH       = "/tmp/q/"
	SYNC_DELAY = time.Millisecond * 1000
	encoder    = binary.BigEndian
	blank      = struct{}{}
)

func init() {
	if err := os.MkdirAll(PATH, 0700); err != nil {
		panic(err)
	}
}

type Topic struct {
	sync.Mutex
	dataLock     sync.RWMutex
	name         string
	state        *State
	channels     map[string]*Channel
	position     *Position
	segment      *Segment
	segments     map[uint64]*Segment
	addChannel   chan string
	channelAdded chan *Channel
	messageAdded chan struct{}
}

func OpenTopic(name string) (*Topic, error) {
	t := &Topic{
		name:         name,
		channels:     make(map[string]*Channel),
		segments:     make(map[uint64]*Segment),
		addChannel:   make(chan string),
		channelAdded: make(chan *Channel),
		messageAdded: make(chan struct{}),
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
		t.segment = openSegment(t, id)
		t.segments[id] = t.segment
	}
	go t.worker()
	return t, nil
}

func (t *Topic) Write(data []byte) {
	l := len(data)
	t.dataLock.Lock()
	offset := t.position.offset
	if offset+4+l > MAX_QUEUE_SIZE {
		t.expand()
		offset = 0
	}
	encoder.PutUint32(t.segment.data[offset:], uint32(l))
	offset += 4
	copy(t.segment.data[offset:], data)
	t.position.offset = offset + l
	t.dataLock.Unlock()
	t.messageAdded <- blank
}

func (t *Topic) Channel(name string) *Channel {
	t.addChannel <- name
	return <-t.channelAdded
}

func (t *Topic) expand() {
	segment := newSegment(t)
	t.segments[segment.id] = segment
	t.segment = segment
	t.position.offset = 0
	t.position.segmentId = segment.id
}

func (t *Topic) worker() {
	for {
		select {
		case name := <-t.addChannel:
			var c = t.channels[name]
			if c != nil {
				log.Printf("multiple instances of channel %s on topic %s\n", name, t.name)
			} else {
				c = newChannel(t)
				c.position = t.state.loadOrCreatePosition(name)
				if c.position.segmentId == 0 {
					t.dataLock.RLock()
					c.position.offset = t.position.offset
					c.position.segmentId = t.position.segmentId
					t.dataLock.RUnlock()
				}
				t.channels[name] = c
			}
			t.channelAdded <- c
		case <-t.messageAdded:
			for _, c := range t.channels {
				c.notify()
			}
		}
	}
}

func (t *Topic) read(position *Position) []byte {
	t.dataLock.RLock()
	defer t.dataLock.RUnlock()
	if position.offset >= t.position.offset {
		return nil
	}
	l := encoder.Uint32(t.segment.data[position.offset:])
	start := position.offset + 4
	end := start + int(l)
	return t.segment.data[start:end]
}
