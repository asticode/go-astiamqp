package astiamqp

import (
	"context"
	"fmt"
	"time"

	"sync"

	"github.com/asticode/go-astilog"
	"github.com/asticode/go-astitools/time"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

// Constants
const (
	sleepBeforeRetryingToConnect = time.Second
)

// AMQP represents a client capable of sending/listening to AMQP queues
type AMQP struct {
	addr           string
	cancel         context.CancelFunc
	ctx            context.Context
	channel        *amqp.Channel
	connection     *amqp.Connection
	consumerId     int
	consumers      []*Consumer
	mc, mp         *sync.Mutex
	oc, os         *sync.Once
	password       string
	producers      []*Producer
	username       string
	wg             *sync.WaitGroup
}

// New creates a new AMQP instance based on a configuration
func New(c Configuration) (a *AMQP) {
	a = &AMQP{
		addr:           c.Addr,
		mc:             &sync.Mutex{},
		mp:             &sync.Mutex{},
		oc:             &sync.Once{},
		os:             &sync.Once{},
		password:       c.Password,
		username:       c.Username,
		wg:             &sync.WaitGroup{},
	}
	return
}

// Close closes amqp properly
func (a *AMQP) Close() error {
	a.oc.Do(func() {
		if a.channel != nil {
			astilog.Debug("astiamqp: closing channel")
			if err := a.channel.Close(); err != nil {
				astilog.Error(errors.Wrap(err, "astiamqp: closing channel failed"))
			}
		}
		if a.connection != nil {
			astilog.Debug("astiamqp: closing connection")
			if err := a.connection.Close(); err != nil {
				astilog.Error(errors.Wrap(err, "astiamqp: closing connection failed"))
			}
		}
	})
	return nil
}

// Stop stops amqp
// It will wait for all consumers to stop handling deliveries
func (a *AMQP) Stop() {
	astilog.Debug("astiamqp: stopping amqp")
	a.os.Do(func() {
		a.cancel()
		astilog.Debug("astiamqp: waiting for all consumers to stop handling deliveries")
		a.wg.Wait()
		astilog.Debug("astiamqp: all consumers have stopped handling deliveries")
	})
}

// Init initializes amqp
func (a *AMQP) Init(ctx context.Context) (err error) {
	// Set context
	a.ctx, a.cancel = context.WithCancel(ctx)

	// Reset
	if err = a.reset(); err != nil {
		err = errors.Wrap(err, "astiamqp: resetting failed")
		return
	}
	return
}

func (a *AMQP) reset() (err error) {
	// Connect
	if err = a.connect(); err != nil {
		err = errors.Wrap(err, "astiamqp: connecting failed")
		return
	}

	// Handle errors
	c := make(chan *amqp.Error)
	go a.handleErrors(c)

	// Notify close errors
	a.connection.NotifyClose(c)

	// Set up
	if err = a.setup(); err != nil {
		err = errors.Wrap(err, "astiamqp: setting up failed")
		return
	}
	return
}

func (a *AMQP) connect() (err error) {
	astilog.Debug("astiamqp: connecting to AMQP server")
	first := true
	for {
		// Check context
		if err = a.ctx.Err(); err != nil {
			astilog.Debugf("astiamqp: %s, cancelling connect", err)
			return
		}

		// Sleep before retrying except the first time
		if !first {
			astilog.Debugf("astiamqp: sleeping %s before retrying to connect to the AMQP server", sleepBeforeRetryingToConnect)
			if err = astitime.Sleep(a.ctx, sleepBeforeRetryingToConnect); err != nil {
				astilog.Debugf("astiamqp: %s, cancelling connect", err)
				return
			}
		} else {
			first = false
		}

		// Dial
		if a.connection, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s", a.username, a.password, a.addr)); err != nil {
			astilog.Error(errors.Wrapf(err, "astiamqp: dialing AMQP server %s failed", a.addr))
			continue
		}

		// Retrieve channel
		if a.channel, err = a.connection.Channel(); err != nil {
			astilog.Error(errors.Wrap(err, "astiamqp: retrieving AMQP channel failed"))
			continue
		}
		return
	}
}

func (a *AMQP) handleErrors(c chan *amqp.Error) {
	for {
		select {
		case err := <-c:
			astilog.Error(errors.Wrapf(err, "astiamqp: close error"))
			a.reset()
			return
		case <-a.ctx.Done():
			return
		}
	}
}

func (a *AMQP) setup() (err error) {
	// Set up consumers
	if err = a.setupConsumers(); err != nil {
		err = errors.Wrap(err, "astiamqp: setting up consumers failed")
		return
	}

	// Set up producers
	if err = a.setupProducers(); err != nil {
		err = errors.Wrap(err, "astiamqp: setting up producers failed")
		return
	}
	return
}

func (a *AMQP) setupConsumers() (err error) {
	// Lock
	a.mc.Lock()
	defer a.mc.Unlock()

	// Loop through consumers
	for _, c := range a.consumers {
		if err = a.setupConsumer(c); err != nil {
			err = errors.Wrapf(err, "astiamqp: setting up consumer %+v failed", c.configuration)
			return
		}
	}
	return
}

func (a *AMQP) setupProducers() (err error) {
	// Lock
	a.mp.Lock()
	defer a.mp.Unlock()

	// Loop through producers
	for _, p := range a.producers {
		if err = a.setupProducer(p); err != nil {
			err = errors.Wrapf(err, "astiamqp: setting up producer %+v failed", p.configuration)
			return
		}
	}
	return
}
