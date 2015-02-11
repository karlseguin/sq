package garbage4

import (
	"sync"
	"time"
)

type Handler func(message []byte) error

type Channel struct {
	topic    *Topic
	position *Position
	rlock    sync.Mutex
	cond     *sync.Cond
	handler  Handler
	waiting  int
}

func newChannel(topic *Topic) *Channel {
	c := &Channel{
		topic: topic,
	}
	c.cond = &sync.Cond{L: &c.rlock}
	return c
}

func (c *Channel) Consume(handler Handler) {
	c.handler = handler
	for {
		message := c.topic.read(c.position)
		if message == nil && c.topic.align(c) {
			break
		}
		c.handle(message)
	}

	c.rlock.Lock()
	for {
		c.cond.Wait()
		c.rlock.Unlock()
		for {
			message := c.topic.read(c.position)
			if message == nil {
				panic("should not happen //todo: handle better")
			}
			if c.handle(message) == false {
				continue
			}
			c.rlock.Lock()
			c.waiting -= 1
			if c.waiting == 0 {
				break
			}
			c.rlock.Unlock()
		}
	}
}

func (c *Channel) handle(message []byte) bool {
	if err := c.handler(message); err != nil {
		time.Sleep(time.Second) //todo: better
		return false
	}
	c.position.offset += uint32(len(message) + 4)
	return true
}

func (c *Channel) notify(count int) {
	c.rlock.Lock()
	defer c.rlock.Unlock()
	c.waiting += count
	c.cond.Signal()
}

func (c *Channel) aligned() {
	c.rlock.Lock()
	defer c.rlock.Unlock()
	c.waiting = 0
}
