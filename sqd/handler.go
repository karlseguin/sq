package sqd

import (
	"github.com/karlseguin/sq"
	"net"
	"time"
)

type Handler interface {
	Run()
}

func HandlerFactory(conn net.Conn, server *Server) Handler {
	client := NewClient(conn, server)
	client.SetDeadline(time.Now().Add(time.Second * 10))
	message := client.ReadMessage()
	if message == nil {
		return nil
	}

	switch message.Type() {
	case PublisherIntentType:
		return NewPublisher(client, message.(*PublisherIntent))
	}

	return nil
}

type Publisher struct {
	name   string
	client *Client
	topics []*sq.Topic
}

func NewPublisher(client *Client, message *PublisherIntent) *Publisher {
	l := len(message.Topics)
	if l == 0 {
		client.Error("must have at least 1 topic", nil)
		return nil
	}
	topics := make([]*sq.Topic, l)
	for i, topicName := range message.Topics {
		topic, err := client.server.GetTopic(topicName)
		if err != nil {
			client.Error("failed to get or create "+topicName, nil)
			return nil
		}
		topics[i] = topic
	}

	return &Publisher{
		client: client,
		topics: topics,
		name:   message.Name,
	}
}

func (p *Publisher) Run() {
	for {
		p.client.SetDeadline(time.Now().Add(time.Minute))
		message := p.client.ReadMessage()
		if message == nil {
			break
		}
	}
}
