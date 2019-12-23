package astiamqp

import (
	"context"
	"math"
	"strconv"
	"sync"

	"github.com/asticode/go-astikit"
	"github.com/asticode/go-astilog"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Consumer represents a Consumer
type Consumer struct {
	cancel             context.CancelFunc
	ctx                context.Context
	configuration      ConfigurationConsumer
	handlingDeliveries bool
	tag                string
	wg                 *sync.WaitGroup
}

// AddConsumer adds a consumer
func (a *AMQP) AddConsumer(c ConfigurationConsumer) (err error) {
	// Lock
	a.mc.Lock()
	defer a.mc.Unlock()

	// Create consumer
	a.consumerID++
	var csm = &Consumer{
		configuration: c,
		tag:           strconv.Itoa(a.consumerID),
		wg:            &sync.WaitGroup{},
	}
	csm.ctx, csm.cancel = context.WithCancel(a.ctx)

	// Set up consumer
	if err = a.setupConsumer(csm); err != nil {
		err = errors.Wrapf(err, "astiamqp: setting up consumer %+v failed", csm)
		return
	}

	// Append consumer
	a.consumers = append(a.consumers, csm)
	return
}

func (a *AMQP) setupConsumer(c *Consumer) (err error) {
	// Stop handling deliveries
	if c.handlingDeliveries {
		c.cancel()
		c.wg.Wait()
	}

	// Reset context
	c.ctx, c.cancel = context.WithCancel(a.ctx)

	// Declare exchange
	if err = a.declareExchange(c.configuration.Exchange); err != nil {
		err = errors.Wrapf(err, "astiamqp: declaring exchange %+v failed", c.configuration.Exchange)
		return
	}

	// Declare queue
	if err = a.declareQueue(c.configuration.Queue); err != nil {
		err = errors.Wrapf(err, "astiamqp: declaring queue %+v failed", c.configuration.Queue)
		return
	}

	// Bind queue
	if err = a.bindQueue(c.configuration.Queue, c.configuration.Exchange, c.configuration.RoutingKey); err != nil {
		err = errors.Wrapf(err, "astiamqp: binding queue %+v to exchange %+v for routing key %s failed", c.configuration.Queue, c.configuration.Exchange, c.configuration.RoutingKey)
		return
	}

	// Consume
	var deliveries <-chan amqp.Delivery
	if deliveries, err = a.consume(c); err != nil {
		err = errors.Wrapf(err, "astiamqp: consuming on consumer %+v failed", c.configuration)
		return
	}

	// Handle deliveries
	astilog.Debugf("astiamqp: handling deliveries of consumer %s on queue %s", c.tag, c.configuration.Queue.Name)
	go func() {
		// Handle waiting groups
		c.handlingDeliveries = true
		a.wg.Add(1)
		c.wg.Add(1)
		defer func() {
			a.wg.Done()
			c.wg.Done()
		}()

		// Loop
		for {
			select {
			case d := <-deliveries:
				if d.DeliveryTag > 0 {
					astilog.Debugf("astiamqp: received body %s on routing key %s, queue %s and exchange %s", string(d.Body), d.RoutingKey, c.configuration.Queue.Name, c.configuration.Exchange.Name)
					if err = c.configuration.Handler(d.Body, d.RoutingKey, acknowledger{deliveryTag: d.DeliveryTag, acknowledger: d.Acknowledger}); err != nil {
						astilog.Error(errors.Wrapf(err, "astiamqp: handling body %s on routing key %s, queue %s and exchange %s", string(d.Body), d.RoutingKey, c.configuration.Queue.Name, c.configuration.Exchange.Name))
					}
				}
			case <-c.ctx.Done():
				astilog.Debugf("astiamqp: stopping handling deliveries for consumer %s", c.tag)
				return
			}
		}
	}()
	return
}

func (a *AMQP) consume(c *Consumer) (deliveries <-chan amqp.Delivery, err error) {
	astilog.Debugf("astiamqp: consuming on queue %s with consumer %s", c.configuration.Queue.Name, c.tag)
	if deliveries, err = a.channel.Consume(
		c.configuration.Queue.Name,            // queue
		c.tag,                                 // consumer
		c.configuration.AutoAck,               // auto-ack
		c.configuration.Exclusive,             // exclusive
		c.configuration.NoLocal,               // no-local
		c.configuration.NoWait,                // no-wait
		amqp.Table(c.configuration.Arguments), // args
	); err != nil {
		err = errors.Wrapf(err, "astiamqp: consuming on consumer %+v failed", c.configuration)
		return
	}
	return
}

// ConsumeOptions represents consume options
type ConsumeOptions struct {
	Consumer    ConfigurationConsumer
	WorkerCount int
}

// Consume consumes AMQP events
func (a *AMQP) Consume(w *astikit.Worker, cs ...ConsumeOptions) (err error) {
	// No options
	if len(cs) == 0 {
		return
	}

	// Loop through configurations
	for idxConf, c := range cs {
		// Loop through workers
		for idxWorker := 0; idxWorker < int(math.Max(1, float64(c.WorkerCount))); idxWorker++ {
			if err = a.AddConsumer(c.Consumer); err != nil {
				err = errors.Wrapf(err, "main: adding consumer #%d for conf #%d %+v failed", idxWorker+1, idxConf+1, c)
				return
			}
		}
	}

	// Execute in a task
	w.NewTask().Do(func() {
		// Wait for context to be done
		<-w.Context().Done()

		// Stop amqp
		a.Stop()
	})
	return
}
