package sqd

import (
	"github.com/karlseguin/sq"
	"log"
	"net"
	"sync"
)

func Listen(config *Configuration) {
	server := NewServer(config)

	socket, err := net.Listen("tcp", config.Address)
	if err != nil {
		panic(err)
	}
	for {
		if conn, err := socket.Accept(); err != nil {
			log.Println("socket access", err)
		} else {
			go handler(conn, server)
		}
	}
}

func handler(conn net.Conn, server *Server) {
	handler := HandlerFactory(conn, server)
	if handler != nil {
		handler.Run()
	}
}

type Server struct {
	sync.Mutex
	config *Configuration
	topics map[string]*sq.Topic
}

func NewServer(config *Configuration) *Server {
	return &Server{
		config: config,
		topics: make(map[string]*sq.Topic),
	}
}

func (s *Server) GetTopic(name string) (*sq.Topic, error) {
	defer s.Unlock()
	s.Lock()
	topic, exists := s.topics[name]
	if exists {
		return topic, nil
	}
	topic, err := sq.OpenTopic(name, s.config.Topics)
	if err != nil {
		s.topics[name] = topic
	}
	return topic, err
}
