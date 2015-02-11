package garbage4

import (
	"sync"
	"time"
)

type Handler func(message []byte) error

type Channel struct {
	sync.Mutex
	name     string
	topic    *Topic
	position *Position
	cond     *sync.Cond
	handler  Handler
	waiting  int
}

func newChannel(topic *Topic, name string) *Channel {
	c := &Channel{
		name:  name,
		topic: topic,
	}
	c.cond = &sync.Cond{L: &c.Mutex}
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

	c.Lock()
	for {
		for c.waiting == 0 {
			c.cond.Wait()
		}
		c.Unlock()
		for {
			message := c.topic.read(c.position)
			if message == nil {
				panic("should not happen //todo: handle better: " + c.name)
			}
			if c.handle(message) == false {
				continue
			}
			c.Lock()
			c.waiting -= 1
			if c.waiting == 0 {
				break
			}
			c.Unlock()
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
	c.Lock()
	defer c.Unlock()
	c.waiting += count
	c.cond.Signal()
}

func (c *Channel) aligned() {
	c.Lock()
	defer c.Unlock()
	c.waiting = 0
}
