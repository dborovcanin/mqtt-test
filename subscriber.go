package mqtt

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Subscriber interface {
	Subscribe()
}

type subscriber struct {
	id          string
	client      mqtt.Client
	numMessages int
	qos         byte
	topic       string
}

type SubscriberConfig struct {
	ID          string
	NumMessages int
	Timeout     time.Duration
	QoS         byte
	Topic       string
}

func NewSubscriber(cli mqtt.Client, cfg SubscriberConfig) Subscriber {
	return &subscriber{
		client:      cli,
		id:          cfg.ID,
		qos:         cfg.QoS,
		topic:       cfg.Topic,
		numMessages: cfg.NumMessages,
	}
}

func (s *subscriber) Subscribe() {
	s.client.Subscribe(s.topic, s.qos, func(cli mqtt.Client, msg mqtt.Message) {
		s.numMessages++
	})
}
