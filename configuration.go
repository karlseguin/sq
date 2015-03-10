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
	temp      bool
	batchSize int
}

func ConfigureChannel() *ChannelConfiguration {
	return &ChannelConfiguration{
		batchSize: 1,
	}
}

func (c *ChannelConfiguration) BatchSize(size uint16) *ChannelConfiguration {
	c.batchSize = int(size)
	return c
}

func (c *ChannelConfiguration) Temp() *ChannelConfiguration {
	c.temp = true
	return c
}
