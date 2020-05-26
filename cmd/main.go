package main

import (
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/mqtt-test"
)

var payload = `const payload = Apache License

const (
	url       = "64.225.82.174:1883"
	numClient = 5000
	timeout   = time.Second * 40
	keepAlive = time.Second * 3 * 30
)

const (
	username = "3720905b-ec0c-4a93-a97c-e2992af0396d"
	pass     = "9de0f4db-ebff-4c84-94aa-a81c4443281f"
	channel  = "848ecb4c-ea25-4462-95c0-6dadbf76335f"
)

func createClient(id, username, pass string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(url).
		SetAutoReconnect(true).
		SetClientID(id).
		SetUsername(username).
		SetPassword(pass).
		SetKeepAlive(keepAlive)
	cli := mqtt.NewClient(opts)
	tkn := cli.Connect()`

const (
	url       = "178.62.252.95:1883"
	numPubs   = 4
	msgPerPub = 100
	totalMsgs = numPubs * msgPerPub
	timeout   = time.Millisecond * 1000
	keepAlive = time.Second * 10
)

const (
	username = "81755d18-2b6a-4c1f-a475-0fb263182fb1"
	password = "3f7c0ef2-188b-4b55-9b65-453dff9a07fe"
	channel  = "bc037769-81f3-4ba6-b5ff-48984f4ec0a5"
)

func main() {
	// Increase the payload size (~152.5KB).
	for i := 0; i < 8; i++ {
		payload += payload
	}
	pld := []byte(payload)
	log.Println("Payload size: ", float64(len(pld))/1024, "KB")

	var mu sync.Mutex
	var wg sync.WaitGroup

	resultReceived := make(chan int64)
	runSub(resultReceived)

	var pubs []mqtt.Publisher
	for i := 0; i < numPubs; i++ {
		go addPub(i, pld, &mu, &wg, &pubs)

	}

	wg.Wait()
	resultSent := make(chan int64)
	ch := make(chan mqtt.PubResult)
	go waitPub(ch, resultSent, len(pubs))
	log.Println("Starting publishers: ", len(pubs))
	for _, pub := range pubs {
		go func(pub mqtt.Publisher) {
			ch <- pub.Publish()
		}(pub)
	}
	sent := <-resultSent
	log.Println("Total messages sent: ", sent)
	received := <-resultReceived
	log.Println("Total messages received: ", received)
}

func addPub(i int, payload []byte, mu *sync.Mutex, wg *sync.WaitGroup, pubs *[]mqtt.Publisher) {
	wg.Add(1)
	defer wg.Done()
	id := strconv.Itoa(i)
	log.Println("client", id)
	cli, err := mqtt.CreateClient(id, username, password, url, keepAlive, time.Second*10)
	if err != nil {
		log.Println("Failed to create client", err)
	}
	cfg := mqtt.PublisherConfig{
		ID:          id,
		NumMessages: msgPerPub,
		Payload:     payload,
		QoS:         2,
		Topic:       "channels/" + channel + "/messages/" + id,
		Timeout:     timeout,
		PublishWait: time.Second, // 1mps
	}
	pub := mqtt.NewPublisher(cli, cfg)
	mu.Lock()
	*pubs = append(*pubs, pub)
	mu.Unlock()
}

func runSub(results chan int64) {
	log.Println("Starting subscriber...")
	cli, err := mqtt.CreateClient("SUB", username, password, url, keepAlive, time.Second*10)
	if err != nil {
		log.Println("Failed to create SUB", err)
		return
	}
	ch := make(chan mqtt.SubResult)

	cfg := mqtt.SubscriberConfig{
		ID:    "SUB",
		QoS:   2,
		Topic: "channels/" + channel + "/messages/#",
		Res:   ch,
	}
	sub := mqtt.NewSubscriber(cli, cfg)
	go sub.Subscribe()
	go waitSubs(ch, results)
}

func waitSubs(receive chan mqtt.SubResult, send chan int64) {
	var total int64
	timer := time.NewTimer(time.Minute * 2)
	log.Println("Publisher aggregator started.")
	for {
		select {
		case <-timer.C:
			log.Print("Total messages received in time frame: ", total)
			send <- total
			return
		case res := <-receive:
			total = res.NumMsgs
			if total == totalMsgs {
				log.Print("All the messages received: ", total)
				send <- total
				return
			}
		}
	}
}

func waitPub(ch chan mqtt.PubResult, result chan int64, num int) {
	var total int64
	for i := 0; i < num; i++ {
		r := <-ch
		log.Println("Publisher "+r.ID+" finished, sent: ", r.Num)
		total += r.Num
	}
	result <- total
}
