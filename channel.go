package sq

import (
	"sync"
)

type Handler func(message []byte) error

type Channel struct {
	sync.RWMutex
	name    string
	topic   *Topic
	state   *State
	cond    *sync.Cond
	handler Handler
	waiting int
}

func newChannel(topic *Topic, config *ChannelConfiguration) *Channel {
	c := &Channel{
		topic: topic,
		name:  config.name,
	}
	c.cond = &sync.Cond{L: &c.RWMutex}
	return c
}

func (c *Channel) Consume(handler Handler) {
	c.handler = handler
	// drain existing messages
	for {
		if message := c.topic.read(c); message != nil {
			c.handle(message)
		} else {
			break
		}
	}

	c.Lock()
	for {
		for c.waiting == 0 {
			c.cond.Wait()
		}
		c.Unlock()
		processed := 0
		for {
			message := c.topic.read(c)
			if message == nil {
				c.Lock()
				c.waiting -= processed
				break
			}
			if c.handle(message) {
				processed++
			}
		}
	}
}

func (c *Channel) notify() {
	c.Lock()
	c.waiting += 1
	c.Unlock()
	c.cond.Signal()
}

func (c *Channel) handle(message []byte) bool {
	if err := c.handler(message); err != nil {
		return false
	}
	c.Lock()
	c.state.offset += uint32(len(message) + MESSAGE_OVERHEAD)
	c.Unlock()
	return true
}

func (c *Channel) isSegmentUsable(id uint64) bool {
	c.RLock()
	defer c.RUnlock()
	return c.state.isSegmentUsable(id)
}

func (c *Channel) changeSegment(segment *Segment) {
	c.Lock()
	c.state.segmentId = segment.id
	c.state.offset = SEGMENT_HEADER_SIZE
	c.Unlock()
}
