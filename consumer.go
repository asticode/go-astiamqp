package astiamqp

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/streadway/amqp"
)

// Consumer represents a Consumer
type Consumer struct {
	cancel        context.CancelFunc
	ctx           context.Context
	configuration ConfigurationConsumer
	tag           string
}

// AddConsumer adds a consumer
func (a *AMQP) AddConsumer(c ConfigurationConsumer) (err error) {
	// Lock
	a.mc.Lock()
	defer a.mc.Unlock()

	// Create consumer
	var csm = &Consumer{
		configuration: c,
		tag:           strconv.Itoa(int(atomic.AddUint32(&a.consumerCounter, 1))),
	}

	// Set up consumer
	if err = a.setupConsumer(csm); err != nil {
		err = fmt.Errorf("astiamqp: setting up consumer %+v failed: %w", csm, err)
		return
	}

	// Append consumer
	a.consumers = append(a.consumers, csm)
	return
}

func (a *AMQP) setupConsumer(c *Consumer) (err error) {
	// No channel
	if a.channel == nil {
		return
	}

	// Declare exchange
	if err = a.declareExchange(c.configuration.Exchange); err != nil {
		err = fmt.Errorf("astiamqp: declaring exchange %+v failed: %w", c.configuration.Exchange, err)
		return
	}

	// Declare queue
	if err = a.declareQueue(c.configuration.Queue); err != nil {
		err = fmt.Errorf("astiamqp: declaring queue %+v failed: %w", c.configuration.Queue, err)
		return
	}

	// Bind queue
	if err = a.bindQueue(c.configuration.Queue, c.configuration.Exchange, c.configuration.RoutingKey); err != nil {
		err = fmt.Errorf("astiamqp: binding queue %+v to exchange %+v for routing key %s failed: %w", c.configuration.Queue, c.configuration.Exchange, c.configuration.RoutingKey, err)
		return
	}

	// Consume
	var deliveries <-chan amqp.Delivery
	if deliveries, err = a.consume(c); err != nil {
		err = fmt.Errorf("astiamqp: consuming on consumer %+v failed: %w", c.configuration, err)
		return
	}

	// Reset context
	c.ctx, c.cancel = context.WithCancel(a.ctx)

	// Handle deliveries
	a.l.Debugf("astiamqp: handling deliveries of consumer %s on queue %s", c.tag, c.configuration.Queue.Name)
	a.t.NewSubTask().Do(func() {
		for {
			select {
			case d := <-deliveries:
				if d.DeliveryTag > 0 {
					a.l.Debugf("astiamqp: received body %s on routing key %s, queue %s and exchange %s", string(d.Body), d.RoutingKey, c.configuration.Queue.Name, c.configuration.Exchange.Name)
					if err = c.configuration.Handler(d.Body, d.RoutingKey, newAcknowledger(d.Acknowledger, d.DeliveryTag, a.l)); err != nil {
						a.l.Error(fmt.Errorf("astiamqp: handling body %s on routing key %s, queue %s and exchange %s: %w", string(d.Body), d.RoutingKey, c.configuration.Queue.Name, c.configuration.Exchange.Name, err))
					}
				}
			case <-c.ctx.Done():
				a.l.Debugf("astiamqp: stopping handling deliveries for consumer %s", c.tag)
				return
			}
		}
	})
	return
}

func (c *Consumer) stop() {
	if c.cancel != nil {
		c.cancel()
	}
}

func (a *AMQP) consume(c *Consumer) (deliveries <-chan amqp.Delivery, err error) {
	a.l.Debugf("astiamqp: consuming on queue %s with consumer %s", c.configuration.Queue.Name, c.tag)
	if deliveries, err = a.channel.Consume(
		c.configuration.Queue.Name,            // queue
		c.tag,                                 // consumer
		c.configuration.AutoAck,               // auto-ack
		c.configuration.Exclusive,             // exclusive
		c.configuration.NoLocal,               // no-local
		c.configuration.NoWait,                // no-wait
		amqp.Table(c.configuration.Arguments), // args
	); err != nil {
		err = fmt.Errorf("astiamqp: consuming on consumer %+v failed: %w", c.configuration, err)
		return
	}
	return
}
