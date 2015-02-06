package garbage4

import (
	"encoding/binary"
	"sync"
)

const (
	maxQueueSize = 16384
)

var (
	encoder = binary.BigEndian
)

type Topic struct {
	dlock     sync.RWMutex
	olock     sync.RWMutex
	name      string
	offset    int
	current   *Storage
	observers []Observer
	storages  map[int]*Storage
}

func NewTopic(name string) *Topic {
	t := &Topic{
		name:     name,
		storages: make(map[int]*Storage),
	}
	t.expand()
	return t
}

func (t *Topic) Write(data []byte) {
	l := len(data)
	t.dlock.Lock()
	if l+t.offset > maxQueueSize {
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
	index := 0
	if t.current != nil {
		index = t.current.index
	}
	storage := newStorage(t.name, index)
	t.storages[storage.index] = storage
	t.current = storage
	t.offset = 0
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
