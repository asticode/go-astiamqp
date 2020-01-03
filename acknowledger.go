package astiamqp

import (
	"fmt"

	"github.com/asticode/go-astikit"
	"github.com/streadway/amqp"
)

// Acknowledger represents an acknowledger
type Acknowledger interface {
	Ack(multiple bool) error
	Nack(multiple bool, requeue bool) error
	Reject(requeue bool) error
}

type acknowledger struct {
	acknowledger amqp.Acknowledger
	deliveryTag  uint64
	l            astikit.SeverityLogger
}

func newAcknowledger(a amqp.Acknowledger, d uint64, l astikit.SeverityLogger) acknowledger {
	return acknowledger{
		acknowledger: a,
		deliveryTag:  d,
		l:            l,
	}
}

// Ack implements the Acknowledger interface
func (a acknowledger) Ack(multiple bool) (err error) {
	a.l.Debugf("astiamqp: ack %v with multiple %v", a.deliveryTag, multiple)
	if err = a.acknowledger.Ack(a.deliveryTag, multiple); err != nil {
		err = fmt.Errorf("astiamqp: ack %v with multiple %v failed: %w", a.deliveryTag, multiple, err)
		return
	}
	return
}

// Nack implements the Acknowledger interface
func (a acknowledger) Nack(multiple bool, requeue bool) (err error) {
	a.l.Debugf("astiamqp: nack %v with multiple %v and requeue %v", a.deliveryTag, multiple, requeue)
	if err = a.acknowledger.Nack(a.deliveryTag, multiple, requeue); err != nil {
		err = fmt.Errorf("astiamqp: ack %v with multiple %v and requeue %v failed: %w", a.deliveryTag, multiple, requeue, err)
		return
	}
	return
}

// Reject implements the Acknowledger interface
func (a acknowledger) Reject(requeue bool) (err error) {
	a.l.Debugf("astiamqp: reject %v with requeue %v", a.deliveryTag, requeue)
	if err = a.acknowledger.Reject(a.deliveryTag, requeue); err != nil {
		err = fmt.Errorf("astiamqp: reject %v with requeue %v failed: %w", a.deliveryTag, requeue, err)
		return
	}
	return
}
