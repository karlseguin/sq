package garbage4

import (
	"encoding/binary"
	"os"
	"sync"
)

const (
	MAX_QUEUE_SIZE = 16384
)

var (
	PATH    = "/tmp/q/"
	encoder = binary.BigEndian
)

func init() {
	if err := os.MkdirAll(PATH, 0700); err != nil {
		panic(err)
	}
}

type Topic struct {
	dlock     sync.RWMutex
	olock     sync.RWMutex
	name      string
	offset    int
	current   *Storage
	observers []Observer
	storages  map[uint64]*Storage
}

func OpenTopic(name string) *Topic {
	t := &Topic{
		name:     name,
		storages: make(map[uint64]*Storage),
	}
	if loadState(t) == false {
		t.expand()
	}
	return t
}

func (t *Topic) Write(data []byte) {
	l := len(data)
	t.dlock.Lock()
	if l+t.offset+4 > MAX_QUEUE_SIZE {
		t.expand()
	}
	encoder.PutUint32(t.current.data[t.offset:], uint32(l))
	t.offset += 4
	copy(t.current.data[t.offset:], data)
	t.offset += l
	t.dlock.Unlock()

	t.olock.RLock()
	defer t.olock.RUnlock()
	for _, o := range t.observers {
		o.Notify()
	}
}

func (t *Topic) expand() {
	storage := newStorage(t)
	t.storages[storage.id] = storage
	t.current = storage
	t.offset = 0
	saveState(t)
}

func (t *Topic) catchup(c *Channel) []byte {
	//important that this locks get held until we've added the observer
	t.dlock.RLock()
	defer t.dlock.RUnlock()

	if message := t.lockedRead(c.position); message != nil {
		return message
	}
	t.olock.Lock()
	defer t.olock.Unlock()
	t.observers = append(t.observers, c)
	return nil
}

func (t *Topic) read(position Position) []byte {
	t.dlock.RLock()
	defer t.dlock.RUnlock()
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
