package rabbit

import "github.com/streadway/amqp"

func (con *Connector) CreateQueue(name string, durable bool, priority int) error {
	args := amqp.Table{}
	if priority != 0 {
		args["x-max-priority"] = priority
	}
	con.logger.Infof("Create queue (%s), durable: %v, priority: %d", name, durable, priority)
	_, err := con.channel.QueueDeclare(name, durable, false, false, false, args)
	// Success
	return err
}

func (con *Connector) DeleteQueue(name string) error {
	total, err := con.channel.QueueDelete(name, false, false, false)
	con.logger.Infof("Delete queue (%s), remain messages: %d", name, total)
	// Success
	return err
}

func (con *Connector) BindQueueToExchange(queue, exchange, key string) error {
	// Success
	con.logger.Infof("Bind queue (%s) to exchange (%s) with key: %s", queue, exchange, key)
	return con.channel.QueueBind(queue, key, exchange, false, nil)
}

func (con *Connector) UnbindQueueToExchange(queue, exchange, key string) error {
	// Success
	con.logger.Infof("Unbind queue (%s) to exchange (%s) with key: %s", queue, exchange, key)
	return con.channel.QueueUnbind(queue, key, exchange, nil)
}
