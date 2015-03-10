package sq

type TopicConfiguration struct {
	path        string
	segmentSize int
}

func ConfigureTopic() *TopicConfiguration {
	return &TopicConfiguration{
		path:        "/tmp/q",
		segmentSize: 16777216,
	}
}

// The path to store the data (all files belonging to a topic will be located
// at <PATH>/TOPIC_NAME/)
// [/tmp/q]
func (c *TopicConfiguration) Path(path string) *TopicConfiguration {
	c.path = path
	return c
}

// The size of each segment file, in bytes
// [16777216]  (16MB)
func (c *TopicConfiguration) SegmentSize(size uint32) *TopicConfiguration {
	c.segmentSize = int(size)
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

// Creates a temporary channel. Temporary channels do not maintain their position
// when closed
func (c *ChannelConfiguration) Temp() *ChannelConfiguration {
	c.temp = true
	return c
}
