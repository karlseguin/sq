package garbage4

type Configuration struct {
	path string
}

func Configure() *Configuration {
	return &Configuration{
		path: "/tmp/q",
	}
}

func (c *Configuration) Path(path string) *Configuration {
	c.path = path
	return c
}
