package sqd

import (
	"errors"
)

type MessageType byte

const (
	PublisherIntentType MessageType = iota
)

var (
	UnknownMessageTypeErr = errors.New("Unknown message type")
)

type Message interface {
	Type() MessageType
}

func MessageFactory(r *Reader) (Message, error) {
	switch MessageType(r.Byte()) {
	case PublisherIntentType:
		return HydratePublisherIntent(r)
	}
	return nil, UnknownMessageTypeErr
}

type PublisherIntent struct {
	Name   string
	Topics []string
}

func HydratePublisherIntent(r *Reader) (*PublisherIntent, error) {
	name, err := r.String()
	if err != nil {
		return nil, err
	}
	topics, err := r.Strings()
	if err != nil {
		return nil, err
	}
	return &PublisherIntent{name, topics}, nil
}

func (m *PublisherIntent) Type() MessageType {
	return PublisherIntentType
}
