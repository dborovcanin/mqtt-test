package mqtt

import (
	"log"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Subscriber interface {
	Subscribe()
}

type SubResult struct {
	ID      string
	NumMsgs int64
}

type SubscriberConfig struct {
	ID    string
	QoS   byte
	Topic string
	Res   chan SubResult
}

type subscriber struct {
	id          string
	client      mqtt.Client
	numMessages *int64
	qos         byte
	topic       string
	res         chan SubResult
}

func NewSubscriber(cli mqtt.Client, cfg SubscriberConfig) Subscriber {
	var numMessages int64
	ret := &subscriber{
		client:      cli,
		id:          cfg.ID,
		qos:         cfg.QoS,
		topic:       cfg.Topic,
		res:         cfg.Res,
		numMessages: &numMessages,
	}

	go func() {
		ticker := time.NewTicker(time.Second * 15)
		for {
			<-ticker.C
			received := atomic.LoadInt64(ret.numMessages)
			log.Println("Recevied: ", received)
		}
	}()
	return ret
}

func (s *subscriber) Subscribe() {
	s.client.Subscribe(s.topic, s.qos, func(cli mqtt.Client, msg mqtt.Message) {
		num := atomic.AddInt64(s.numMessages, 1)
		s.res <- SubResult{s.id, num}
	})
}
