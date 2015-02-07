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
	lock sync.RWMutex
	name string

	allChannels    map[string]*Channel
	activeChannels []*Channel

	offset   int
	segment  *Segment
	segments map[uint64]*Segment

	channelAdded chan *Channel
	messageAdded chan struct{}
}

func OpenTopic(name string) *Topic {
	t := &Topic{
		name:         name,
		segments:     make(map[uint64]*Segment),
		channelAdded: make(chan *Channel),
		messageAdded: make(chan struct{}),
	}
	if loadState(t) == false {
		t.expand()
	}
	go t.worker()
	return t
}

func (t *Topic) Write(data []byte) {
	l := len(data)
	t.lock.Lock()
	if t.offset+4 > MAX_QUEUE_SIZE {
		t.expand()
	}
	encoder.PutUint32(t.segment.data[t.offset:], uint32(l))
	t.offset += 4
	copy(t.segment.data[t.offset:], data)
	t.offset += l
	t.lock.Unlock()
	t.messageAdded <- blank
}

func (t *Topic) Channel(name string) *Channel {
	c := newChannel(t)
	t.lock.RLock()
	c.position = Position{
		id:     t.segment.id,
		offset: t.offset,
	}
	t.lock.RUnlock()
	t.channelAdded <- c
	return c
}

func (t *Topic) expand() {
	segment := newSegment(t)
	t.segments[segment.id] = segment
	t.segment = segment
	t.offset = 0
	saveState(t, false)
}

func (t *Topic) worker() {
	timer := time.NewTimer(SYNC_DELAY)
	for {
		select {
		case c := <-t.channelAdded:
			t.activeChannels = append(t.activeChannels, c)
		case <-t.messageAdded:
			for _, c := range t.activeChannels {
				c.Notify()
			}
		case <-timer.C:
			t.lock.RLock()
			saveState(t, true)
			t.lock.RUnlock()
			timer.Reset(SYNC_DELAY)
		}
	}
}

func (t *Topic) catchup(c *Channel) []byte {
	//important that this locks get held until we've added the observer
	t.lock.RLock()
	defer t.lock.RUnlock()

	if message := t.lockedRead(c.position); message != nil {
		return message
	}
	return nil
}

func (t *Topic) read(position Position) []byte {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.lockedRead(position)
}

func (t *Topic) lockedRead(position Position) []byte {
	if position.offset >= t.offset {
		return nil
	}

	l := encoder.Uint32(t.segment.data[position.offset:])
	start := position.offset + 4
	end := start + int(l)
	return t.segment.data[start:end]
}
