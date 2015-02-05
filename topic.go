package garbage4

import (
	"encoding/binary"
	"os"
	"sync"
	"syscall"
	"unsafe"
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
	ref       []byte
	name      string
	offset    int
	data      *[maxQueueSize]byte
	observers []Observer
}

func NewTopic(name string) *Topic {
	t := &Topic{
		name: name,
	}

	file, err := os.OpenFile(name+".q", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	file.Truncate(maxQueueSize)
	b, err := syscall.Mmap(int(file.Fd()), 0, maxQueueSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	t.ref = b
	t.data = (*[maxQueueSize]byte)(unsafe.Pointer(&b[0]))
	return t
}

func (t *Topic) Write(data []byte) {
	l := len(data)
	t.dlock.Lock()
	encoder.PutUint32(t.data[t.offset:], uint32(l))
	t.offset += 4
	copy(t.data[t.offset:], data)
	t.offset += l
	t.dlock.Unlock()

	t.olock.RLock()
	defer t.olock.RUnlock()
	for _, o := range t.observers {
		o.Notify()
	}
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

	l := encoder.Uint32(t.data[position.offset:])
	start := position.offset + 4
	end := start + int(l)
	return t.data[start:end]
}
