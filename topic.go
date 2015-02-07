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
	lock         sync.RWMutex
	name         string
	offset       int
	current      *Storage
	channels     []*Channel
	storages     map[uint64]*Storage
	channelAdded chan *Channel
	messageAdded chan struct{}
}

func OpenTopic(name string) *Topic {
	t := &Topic{
		name:         name,
		storages:     make(map[uint64]*Storage),
		channelAdded: make(chan *Channel),
		messageAdded: make(chan struct{}),
	}
	if loadState(t) == false {
		t.expand()
	}
	return t
}

func (t *Topic) Write(data []byte) {
	l := len(data)
	t.lock.Lock()
	if t.offset+4 > MAX_QUEUE_SIZE {
		t.expand()
	}
	encoder.PutUint32(t.current.data[t.offset:], uint32(l))
	t.offset += 4
	copy(t.current.data[t.offset:], data)
	t.offset += l
	t.lock.Unlock()
	t.messageAdded <- blank
}

func (t *Topic) Channel(name string) *Channel {
	c := newChannel(t)
	t.lock.RLock()
	c.position = Position{
		id:     t.current.id,
		offset: t.offset,
	}
	t.lock.RUnlock()
	t.channelAdded <- c
	return c
}

func (t *Topic) expand() {
	storage := newStorage(t)
	t.storages[storage.id] = storage
	t.current = storage
	t.offset = 0
	saveState(t, false)
}

func (t *Topic) worker() {
	timer := time.NewTimer(SYNC_DELAY)
	for {
		select {
		case c := <-t.channelAdded:
			t.channels = append(t.channels, c)
		case <-t.messageAdded:
			for _, c := range t.channels {
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

	l := encoder.Uint32(t.current.data[position.offset:])
	start := position.offset + 4
	end := start + int(l)
	return t.current.data[start:end]
}
