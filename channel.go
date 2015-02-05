package garbage4

import (
	"sync"
	"time"
)

type Handler func(message []byte) error

type Observer interface {
	Notify()
}

type Channel struct {
	topic    *Topic
	position Position
	lock     sync.Mutex
	cond     *sync.Cond
	handler  Handler
	waiting  int
}

func NewChannel(topic *Topic) *Channel {
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

	for {
		c.lock.Lock()
		c.cond.Wait()
		c.lock.Unlock()
		for {
			message := c.topic.read(c.position)
			if c.handle(message) == false {
				continue
			}
			c.lock.Lock()
			c.waiting -= 1
			waiting := c.waiting
			c.lock.Unlock()

			if waiting == 0 {
				break
			}
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
	offset int
}
