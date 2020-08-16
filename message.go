package rabbit

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

func (con *Connector) PublishMessage(exchange, key string, message []byte, priority uint8, close bool) error {
	if close {
		defer con.connection.Close()
		defer con.channel.Close()
	}
	payload := amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Priority:     priority,
		Body:         message,
	}
	// Success
	return con.channel.Publish(exchange, key, false, false, payload)
}

func (con *Connector) PublishString(exchange, key, message string, priority uint8, close bool) error {
	// Success
	return con.PublishMessage(exchange, key, []byte(message), priority, close)
}

func (con *Connector) PublishObject(exchange, key string, object interface{}, priority uint8, close bool) error {
	message, err := json.Marshal(object)
	if err != nil {
		return err
	}
	// Success
	return con.PublishMessage(exchange, key, message, priority, close)
}
