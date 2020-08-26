package rabbit

import (
	"errors"
	"fmt"
)

var (
	Fanout  = "fanout"
	Direct  = "direct"
	Topic   = "topic"
	Headers = "headers"

	ExchangeType = []string{Fanout, Direct, Topic, Headers}
)

func (con *Connector) CreateExchange(name, kind string, durable bool) error {
	if !stringInSlice(kind, ExchangeType) {
		return errors.New(fmt.Sprintf("invalid exchange type"))
	}
	con.Logger.Infof("Create exchange (%s), type: %s, durable: %v", name, kind, durable)
	// Success
	return con.channel.ExchangeDeclare(name, kind, durable, false, false, false, nil)
}

func (con *Connector) DeleteExchange(name string) error {
	// Success
	con.Logger.Infof("Delete exchange (%s)", name)
	return con.channel.ExchangeDelete(name, false, false)
}

func (con *Connector) BindExchangeToExchange(source, destination, key string) error {
	// Success
	con.Logger.Infof("Bind exchange (%s) to exchange (%s) with key: %s", source, destination, key)
	return con.channel.ExchangeBind(destination, key, source, false, nil)
}

func (con *Connector) UnbindExchangeToExchange(source, destination, key string) error {
	// Success
	con.Logger.Infof("Unbind exchange (%s) to exchange (%s) with key: %s", source, destination, key)
	return con.channel.ExchangeUnbind(destination, key, source, false, nil)
}
