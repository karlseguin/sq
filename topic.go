package garbage4

import (
	"encoding/binary"
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
	dataLock       sync.RWMutex
	name           string
	state          *State
	allChannels    map[string]*Channel
	activeChannels map[string]*Channel
	position       *Position
	segment        *Segment
	segments       map[uint64]*Segment
	channelAdded   chan *Channel
	messageAdded   chan struct{}
}

func OpenTopic(name string) (*Topic, error) {
	t := &Topic{
		name:           name,
		allChannels:    make(map[string]*Channel),
		activeChannels: make(map[string]*Channel),
		segments:       make(map[uint64]*Segment),
		channelAdded:   make(chan *Channel),
		messageAdded:   make(chan struct{}),
	}
	state, err := loadState(t)
	if err != nil {
		return nil, err
	}

	t.state = state
	t.position = state.LoadPosition(0)
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
	println(offset)
	encoder.PutUint32(t.segment.data[offset:], uint32(l))
	offset += 4
	copy(t.segment.data[offset:], data)
	t.position.offset = offset + l
	t.dataLock.Unlock()
	t.messageAdded <- blank
}

func (t *Topic) Channel(name string) *Channel {
	c := newChannel(t)
	// t.dataLock.RLock()
	// c.position = Position{
	// 	id:     t.segment.id,
	// 	offset: t.offset,
	// }
	// t.dataLock.RUnlock()
	// t.channelAdded <- c
	return c
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
		case _ = <-t.channelAdded:
			// t.activeChannels = append(t.activeChannels, c)
		case <-t.messageAdded:
			for _, c := range t.activeChannels {
				c.Notify()
			}
		}
	}
}

func (t *Topic) catchup(c *Channel) []byte {
	// //important that this locks get held until we've added the observer
	// t.dataLock.RLock()
	// defer t.dataLock.RUnlock()

	// if message := t.dataLockedRead(c.position); message != nil {
	// 	return message
	// }
	return nil
}

func (t *Topic) read(position Position) []byte {
	// t.dataLock.RLock()
	// defer t.dataLock.RUnlock()
	// return t.dataLockedRead(position)
	return nil
}

func (t *Topic) lockedRead(position Position) []byte {
	// if position.offset >= t.offset {
	// 	return nil
	// }

	// l := encoder.Uint32(t.segment.data[position.offset:])
	// start := position.offset + 4
	// end := start + int(l)
	// return t.segment.data[start:end]
	return nil
}
