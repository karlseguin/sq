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
