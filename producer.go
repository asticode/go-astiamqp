package astiamqp

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/asticode/go-astikit"
	"github.com/streadway/amqp"
)

// Producer represents a producer
type Producer struct {
	b             []producerBufferItem
	channel       func() *amqp.Channel
	configuration ConfigurationProducer
	l             astikit.SeverityLogger
	m             *sync.Mutex // Locks b
}

type producerBufferItem struct {
	msg        []byte
	routingKey string
}

// NewProducer creates a producer
func (a *AMQP) NewProducer(c ConfigurationProducer) (p *Producer, err error) {
	// Lock
	a.mp.Lock()
	defer a.mp.Unlock()

	// Create producer
	p = &Producer{
		channel:       func() *amqp.Channel { return a.channel },
		configuration: c,
		l:             a.l,
		m:             &sync.Mutex{},
	}

	// Setup producer
	if err = a.setupProducer(p); err != nil {
		err = fmt.Errorf("astiamqp: setting up producer %+v failed: %w", c, err)
		return
	}

	// Append producer
	a.producers = append(a.producers, p)
	return
}

func (a *AMQP) setupProducer(p *Producer) (err error) {
	// No channel
	if a.channel == nil {
		return
	}

	// Declare exchange
	if err = a.declareExchange(p.configuration.Exchange); err != nil {
		err = fmt.Errorf("astiamqp: declaring exchange %+v failed: %w", p.configuration.Exchange, err)
		return
	}

	// Process buffer
	p.m.Lock()
	for _, i := range p.b {
		if errPublish := p.publishMessage(i.msg, i.routingKey); errPublish != nil {
			a.l.Error(fmt.Errorf("astiamqp: publishing msg %+v for routing key %s failed: %w", i.msg, i.routingKey, errPublish))
		}
	}
	p.b = []producerBufferItem{}
	p.m.Unlock()
	return
}

// Produce produces a message on a routing key after json.Marshaling it
func (p *Producer) Produce(msg interface{}, routingKey string) (err error) {
	// Marshal msg
	var b []byte
	if b, err = json.Marshal(msg); err != nil {
		err = fmt.Errorf("astiamqp: marshaling msg %+v failed: %w", msg, err)
		return
	}

	// Publish message
	if err = p.publishMessage(b, routingKey); err != nil {
		err = fmt.Errorf("astiamqp: publishing msg %+v for routing key %s failed: %w", msg, routingKey, err)
		return
	}
	return
}

func (p *Producer) publishMessage(msg []byte, routingKey string) (err error) {
	// Get channel
	c := p.channel()

	// No channel
	if c == nil {
		// Store in buffer
		p.m.Lock()
		p.b = append(p.b, producerBufferItem{
			msg:        msg,
			routingKey: routingKey,
		})
		p.m.Unlock()
		return
	}

	// Publish
	p.l.Debugf("astiamqp: publishing msg %s to exchange %s for routing key %s", msg, p.configuration.Exchange.Name, routingKey)
	if err = c.Publish(
		p.configuration.Exchange.Name, // exchange
		routingKey,                    // routing key
		false,                         // mandatory
		false,                         // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg,
		},
	); err != nil {
		err = fmt.Errorf("astiamqp: publishing msg %s to exchange %+v for routing key %s failed: %w", msg, p.configuration.Exchange, routingKey, err)
		return
	}
	return
}
