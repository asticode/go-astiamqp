package astiamqp

import (
	"encoding/json"

	"github.com/asticode/go-astilog"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Producer represents a producer
type Producer struct {
	channel       func() *amqp.Channel
	configuration ConfigurationProducer
}

// AddProducer adds a producer
func (a *AMQP) AddProducer(c ConfigurationProducer) (p *Producer, err error) {
	// Lock
	a.mp.Lock()
	defer a.mp.Unlock()

	// Create producer
	p = &Producer{
		channel:       func() *amqp.Channel { return a.channel },
		configuration: c,
	}

	// Setup producer
	if err = a.setupProducer(p); err != nil {
		err = errors.Wrapf(err, "astiamqp: setting up producer %+v failed", c)
		return
	}

	// Append producer
	a.producers = append(a.producers, p)
	return
}

func (a *AMQP) setupProducer(p *Producer) (err error) {
	// Declare exchange
	if err = a.declareExchange(p.configuration.Exchange); err != nil {
		err = errors.Wrapf(err, "astiamqp: declaring exchange %+v failed", p.configuration.Exchange)
		return
	}
	return
}

// Produce produces a message on a routing key after json.Marshaling it
func (p *Producer) Produce(msg interface{}, routingKey string) (err error) {
	// Marshal msg
	var b []byte
	if b, err = json.Marshal(msg); err != nil {
		err = errors.Wrapf(err, "astiamqp: marshaling msg %+v failed", msg)
		return
	}

	// Publish message
	if err = p.publishMessage(b, routingKey); err != nil {
		err = errors.Wrapf(err, "astiamqp: publishing msg %+v for routing key %s failed", msg, routingKey)
		return
	}
	return
}

func (p *Producer) publishMessage(msg []byte, routingKey string) (err error) {
	astilog.Debugf("astiamqp: publishing msg %s to exchange %s for routing key %s", msg, p.configuration.Exchange.Name, routingKey)
	if err = p.channel().Publish(
		p.configuration.Exchange.Name, // exchange
		routingKey,                    // routing key
		false,                         // mandatory
		false,                         // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg,
		},
	); err != nil {
		err = errors.Wrapf(err, "astiamqp: publishing msg %s to exchange %+v for routing key %s failed", msg, p.configuration.Exchange, routingKey)
		return
	}
	return
}
