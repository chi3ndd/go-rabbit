package rabbit

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

func (con *Connector) Publish(exchange, key string, message []byte, priority uint8, close bool) error {
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
	return con.Publish(exchange, key, []byte(message), priority, close)
}

func (con *Connector) PublishObject(exchange, key string, object interface{}, priority uint8, close bool) error {
	message, err := json.Marshal(object)
	if err != nil {
		return err
	}
	// Success
	return con.Publish(exchange, key, message, priority, close)
}

func (con *Connector) Consume(queue string, prefectCount int, autoAck bool) (<-chan amqp.Delivery, error) {
	// Success
	if prefectCount != 0 {
		err := con.channel.Qos(prefectCount, 0, false)
		if err != nil {
			return nil, err
		}
	}
	messages, err := con.channel.Consume(queue, "", autoAck, false, false, false, nil)
	if err != nil {
		return nil, err
	}
	// Success
	return messages, err
}
