package mqtt

import (
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Publisher interface {
	Publish() PubResult
}

type PubResult struct {
	ID  string
	Num int64
}

type publisher struct {
	id          string
	client      mqtt.Client
	timeout     time.Duration
	wait        time.Duration
	numMessages int
	payload     []byte
	qos         byte
	topic       string
}

type PublisherConfig struct {
	ID          string
	NumMessages int
	Timeout     time.Duration
	PublishWait time.Duration
	Payload     []byte
	QoS         byte
	Topic       string
}

func NewPublisher(cli mqtt.Client, cfg PublisherConfig) Publisher {
	return &publisher{
		client:      cli,
		id:          cfg.ID,
		numMessages: cfg.NumMessages,
		timeout:     cfg.Timeout,
		payload:     cfg.Payload,
		qos:         cfg.QoS,
		wait:        cfg.PublishWait,
		topic:       cfg.Topic,
	}
}

func (p *publisher) Publish() PubResult {
	var published int64
	var allMsgs int
	for allMsgs < p.numMessages {
		allMsgs++
		tkn := p.client.Publish(p.topic, p.qos, false, p.payload)
		if tkn.Error() != nil {
			log.Printf("WARN: failed to create publish token %s\n", tkn.Error())
			// time.Sleep(p.wait)
			continue
		}
		if tkn.WaitTimeout(p.timeout) {
			if tkn.Error() != nil {
				log.Printf("WARN: failed to publish %s\n", tkn.Error())
			}
			// else {
			// log.Println("Client " + p.id + " published")
			// }
			published++
			// time.Sleep(p.wait)
			continue
		}
		log.Println("WARN: Client " + p.id + " failed to publish due to timeout")
	}
	return PubResult{p.id, published}
}
