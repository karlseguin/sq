### Simple Queue

Just playing around with code. Learn more at:
http://openmymind.net/Building-A-Queue-Part-1/

```go
package main

import (
	"github.com/karlseguin/sq"
	"strconv"
)

func main() {
	topic, err := sq.OpenTopic("sample", sq.Configure().Path("./data/"))
	if err != nil {
		panic(err)
	}
	go func() {
		channel, err := topic.Channel("channel-1")
		if err != nil {
			panic(err)
		}
		channel.Consume(handler)
	}()

	for i := 0; i < 10000; i++ {
		if err := topic.Write([]byte(strconv.Itoa(i))); err != nil {
			panic(err)
		}
	}
}

// returning an error means the message for this channel won't be consumed
func handler(message []byte) error {
	println("handler-a", string(message))
	return nil
}
```
