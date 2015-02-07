package garbage4

import (
	"sync"
	"time"
)

type Handler func(message []byte) error

type Channel struct {
	topic    *Topic
	position Position
	lock     sync.Mutex
	cond     *sync.Cond
	handler  Handler
	waiting  int
}

func newChannel(topic *Topic) *Channel {
	c := &Channel{
		topic:    topic,
		position: Position{},
	}
	c.cond = &sync.Cond{L: &c.lock}
	return c
}

func (c *Channel) Consume(handler Handler) {
	c.handler = handler
	for {
		message := c.topic.catchup(c)
		if message == nil {
			break
		}
		c.handle(message)
	}

	c.lock.Lock()
	for {
		c.cond.Wait()
		c.lock.Unlock()
		for {
			message := c.topic.read(c.position)
			if message == nil {
				panic("should not happen //todo: handle better")
			}
			if c.handle(message) == false {
				continue
			}
			c.lock.Lock()
			c.waiting -= 1
			if c.waiting == 0 {
				break
			}
			c.lock.Unlock()
		}
	}
}

func (c *Channel) handle(message []byte) bool {
	if err := c.handler(message); err != nil {
		time.Sleep(time.Second) //todo: better
		return false
	}
	c.position.offset += len(message) + 4
	return true
}

func (c *Channel) Notify() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.waiting += 1
	c.cond.Signal()
}

type Position struct {
	id     uint64
	offset int
}
