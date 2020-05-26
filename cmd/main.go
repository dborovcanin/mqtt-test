package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/mqtt-test"
)

const payload = `const payload = Apache License

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
	url        = "178.62.252.95:1883"
	numClients = 5000
	timeout    = time.Millisecond * 100
	keepAlive  = time.Second * 10
)

const (
	username = "19944084-faac-498e-9a2b-4d51338df412"
	password = "5b066d21-f9ca-4076-b641-fa93a01c1c9e"
	channel  = "df780968-5de7-4a45-95b1-958d641970d6"
)

func wait(ch, result chan int, num int) {
	var total int
	for i := 0; i < num; i++ {
		r := <-ch
		total += r
	}
	result <- total
}

func main() {
	pld := []byte(payload)
	ch := make(chan int)
	result := make(chan int)
	var clients int
	for i := 0; i < numClients; i++ {
		id := strconv.Itoa(i)
		fmt.Println("client", id)
		cli, err := mqtt.CreateClient(id, username, password, url, keepAlive, time.Second*10)
		if err != nil {
			log.Println("Failed to create client")
			continue
		}
		cfg := mqtt.PublisherConfig{
			ID:          id,
			NumMessages: 100,
			Payload:     pld,
			QoS:         2,
			Topic:       "channels/" + channel + "/messages/" + id,
			Timeout:     timeout,
			PublishWait: time.Second, // 1mps
		}
		pub := mqtt.NewPublisher(cli, cfg)
		clients++
		go func() {
			ch <- pub.Publish()
		}()
	}
	go wait(ch, result, clients)
	total := <-result
	fmt.Println("Total messages sent: ", total)
}
