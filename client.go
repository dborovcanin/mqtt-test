package mqtt

import (
	"errors"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func CreateClient(id, username, pass, url string, keepAlive, timeout time.Duration) (mqtt.Client, error) {
	time.Sleep(time.Second)
	opts := mqtt.NewClientOptions().
		AddBroker(url).
		SetAutoReconnect(true).
		SetClientID(id).
		SetUsername(username).
		SetPassword(pass).
		SetAutoReconnect(false).
		SetKeepAlive(keepAlive)
	cli := mqtt.NewClient(opts)
	tkn := cli.Connect()
	if tkn.Error() != nil {
		return nil, tkn.Error()
	}
	if tkn.WaitTimeout(timeout) {
		if tkn.Error() != nil {
			return nil, tkn.Error()
		}
		return cli, nil
	}
	return nil, errors.New("Unable to connect client")
}
