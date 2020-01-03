package astiamqp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/asticode/go-astikit"
	"github.com/streadway/amqp"
)

// Constants
const (
	sleepBeforeRetryingToConnect = time.Second
)

// AMQP represents a client capable of sending/listening to AMQP queues
type AMQP struct {
	c          Configuration
	cancel     context.CancelFunc
	ctx        context.Context
	channel    *amqp.Channel
	connection *amqp.Connection
	consumerID int
	consumers  []*Consumer
	l          astikit.SeverityLogger
	mc, mp     *sync.Mutex
	oc, os     *sync.Once
	producers  []*Producer
	wg         *sync.WaitGroup
}

// New creates a new AMQP instance based on a configuration
func New(c Configuration) (a *AMQP) {
	a = &AMQP{
		c:  c,
		l:  astikit.AdaptStdLogger(c.Logger),
		mc: &sync.Mutex{},
		mp: &sync.Mutex{},
		oc: &sync.Once{},
		os: &sync.Once{},
		wg: &sync.WaitGroup{},
	}
	return
}

// Close closes amqp properly
func (a *AMQP) Close() error {
	a.oc.Do(func() {
		if a.channel != nil {
			a.l.Debug("astiamqp: closing channel")
			if err := a.channel.Close(); err != nil {
				a.l.Error(fmt.Errorf("astiamqp: closing channel failed: %w", err))
			}
		}
		if a.connection != nil {
			a.l.Debug("astiamqp: closing connection")
			if err := a.connection.Close(); err != nil {
				a.l.Error(fmt.Errorf("astiamqp: closing connection failed: %w", err))
			}
		}
	})
	return nil
}

// Stop stops amqp
// It will wait for all consumers to stop handling deliveries
func (a *AMQP) Stop() {
	a.l.Debug("astiamqp: stopping amqp")
	a.os.Do(func() {
		a.cancel()
		a.l.Debug("astiamqp: waiting for all consumers to stop handling deliveries")
		a.wg.Wait()
		a.l.Debug("astiamqp: all consumers have stopped handling deliveries")
	})
}

// Init initializes amqp
func (a *AMQP) Init(ctx context.Context) (err error) {
	// Set context
	a.ctx, a.cancel = context.WithCancel(ctx)

	// Reset
	if err = a.reset(); err != nil {
		err = fmt.Errorf("astiamqp: resetting failed: %w", err)
		return
	}
	return
}

func (a *AMQP) reset() (err error) {
	// Connect
	if err = a.connect(); err != nil {
		err = fmt.Errorf("astiamqp: connecting failed: %w", err)
		return
	}

	// Handle errors
	c := make(chan *amqp.Error)
	go a.handleErrors(c)

	// Notify close errors
	a.connection.NotifyClose(c)

	// Set up
	if err = a.setup(); err != nil {
		err = fmt.Errorf("astiamqp: setting up failed: %w", err)
		return
	}
	return
}

func (a *AMQP) connect() (err error) {
	a.l.Debug("astiamqp: connecting to AMQP server")
	first := true
	for {
		// Check context
		if err = a.ctx.Err(); err != nil {
			a.l.Debugf("astiamqp: %s, cancelling connect", err)
			return
		}

		// Sleep before retrying except the first time
		if !first {
			a.l.Debugf("astiamqp: sleeping %s before retrying to connect to the AMQP server", sleepBeforeRetryingToConnect)
			if err = astikit.Sleep(a.ctx, sleepBeforeRetryingToConnect); err != nil {
				a.l.Debugf("astiamqp: %s, cancelling connect", err)
				return
			}
		} else {
			first = false
		}

		// Dial
		a.l.Debugf("astiamqp: dialing AMQP server %s", a.c.Addr)
		if a.connection, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s", a.c.Username, a.c.Password, a.c.Addr)); err != nil {
			a.l.Error(fmt.Errorf("astiamqp: dialing AMQP server %s failed: %w", a.c.Addr, err))
			continue
		}

		// Retrieve channel
		a.l.Debug("astiamqp: retrieving AMQP channel")
		if a.channel, err = a.connection.Channel(); err != nil {
			// Close connection
			a.l.Debug("astiamqp: closing connection")
			if err = a.connection.Close(); err != nil {
				a.l.Error(fmt.Errorf("astiamqp: closing connection failed: %w", err))
			}

			// Log
			a.l.Error(fmt.Errorf("astiamqp: retrieving AMQP channel failed: %w", err))
			continue
		}

		// QOS
		if a.c.QOS != nil {
			a.l.Debugf("astiamqp: setting channel qos to %+v", *a.c.QOS)
			if err = a.channel.Qos(a.c.QOS.PrefetchCount, a.c.QOS.PrefetchSize, a.c.QOS.Global); err != nil {
				// Close channel
				a.l.Debug("astiamqp: closing channel")
				if err = a.channel.Close(); err != nil {
					a.l.Error(fmt.Errorf("astiamqp: closing channel failed: %w", err))
				}

				// Close connection
				a.l.Debug("astiamqp: closing connection")
				if err = a.connection.Close(); err != nil {
					a.l.Error(fmt.Errorf("astiamqp: closing connection failed: %w", err))
				}

				// Log
				a.l.Error(fmt.Errorf("astiamqp: setting channel qos to %+v failed: %w", *a.c.QOS, err))
				continue
			}
		}
		return
	}
}

func (a *AMQP) handleErrors(c chan *amqp.Error) {
	for {
		select {
		case err := <-c:
			a.l.Error(fmt.Errorf("astiamqp: close error: %w", err))
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
		err = fmt.Errorf("astiamqp: setting up consumers failed: %w", err)
		return
	}

	// Set up producers
	if err = a.setupProducers(); err != nil {
		err = fmt.Errorf("astiamqp: setting up producers failed: %w", err)
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
			err = fmt.Errorf("astiamqp: setting up consumer %+v failed: %w", c.configuration, err)
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
			err = fmt.Errorf("astiamqp: setting up producer %+v failed: %w", p.configuration, err)
			return
		}
	}
	return
}
