package astiamqp

import (
	"fmt"

	"github.com/streadway/amqp"
)

func (a *AMQP) declareQueue(c ConfigurationQueue) (err error) {
	a.l.Debugf("astiamqp: declaring queue %s", c.Name)
	if _, err = a.channel.QueueDeclare(
		c.Name,                  // name
		c.Durable,               // durable
		c.AutoDeleted,           // delete when unused
		c.Exclusive,             // exclusive
		c.NoWait,                // no-wait
		amqp.Table(c.Arguments), // arguments
	); err != nil {
		err = fmt.Errorf("astiamqp: declaring queue %+v failed: %w", c, err)
		return
	}
	return
}

func (a *AMQP) bindQueue(cq ConfigurationQueue, ce ConfigurationExchange, routingKey string) (err error) {
	a.l.Debugf("astiamqp: binding queue %s to exchange %s with routing key %s", cq.Name, ce.Name, routingKey)
	if err = a.channel.QueueBind(
		cq.Name,                  // queue name
		routingKey,               // routing key
		ce.Name,                  // exchange
		cq.NoWait,                // no-wait
		amqp.Table(cq.Arguments), // arguments
	); err != nil {
		err = fmt.Errorf("astiamqp: binding queue %+v to exchange %+v for routing key %s failed: %w", cq, ce, routingKey, err)
		return
	}
	return
}
