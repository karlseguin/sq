package sq

type TopicConfiguration struct {
	path string
}

func ConfigureTopic() *TopicConfiguration {
	return &TopicConfiguration{
		path: "/tmp/q",
	}
}

func (c *TopicConfiguration) Path(path string) *TopicConfiguration {
	c.path = path
	return c
}

type ChannelConfiguration struct {
	name      string
	batchSize int
}

func ConfigureChannel() *ChannelConfiguration {
	return &ChannelConfiguration{
		batchSize: 1,
	}
}

func (c *ChannelConfiguration) BathSize(size uint16) *ChannelConfiguration {
	c.batchSize = int(size)
	return c
}
