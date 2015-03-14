package sqd

import (
	"errors"
	"github.com/karlseguin/sq"
	"log"
	"net"
	"time"
)

type Client interface {
	Run()
}

func ClientFactory(conn net.Conn, server *Server) Client {
	reader := NewReader(conn, server.config.BufferSize)
	reader.SetDeadline(time.Now().Add(time.Second * 10))
	message, err := reader.ReadMessage()

	if err != nil {
		reader.Close()
		log.Println(err)
		return nil
	}

	var client Client
	switch message.Type() {
	case PublisherIntentType:
		client, err = NewPublisher(message.(*PublisherIntent), reader, server)
	}

	if err == nil && client == nil {
		err = errors.New("initial message must be of type PublisherIntent")
	}
	if err != nil {
		reader.Close()
		log.Println(err)
	}
	return client
}

type Publisher struct {
	name   string
	reader *Reader
	topics []*sq.Topic
}

func NewPublisher(message *PublisherIntent, reader *Reader, server *Server) (*Publisher, error) {
	l := len(message.Topics)
	if l == 0 {
		return nil, errors.New("Publisher intent must intend to publish to at least 1 topic")
	}
	topics := make([]*sq.Topic, l)
	for i, topicName := range message.Topics {
		topic, err := server.GetTopic(topicName)
		if err != nil {
			return nil, err
		}
		topics[i] = topic
	}

	return &Publisher{
		reader: reader,
		topics: topics,
		name:   message.Name,
	}, nil
}

func (p *Publisher) Run() {
	for {
		p.reader.SetDeadline(time.Now().Add(time.Minute))
		_, err := p.reader.ReadMessage()
		if err != nil {
			p.reader.Close()
			break
			// p.Error(err)
			return
		}
	}
}
