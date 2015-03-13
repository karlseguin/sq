package sqd

import (
	"github.com/karlseguin/sq"
)

type Configuration struct {
	Address    string
	Topics     *sq.TopicConfiguration
	BufferSize int
}
