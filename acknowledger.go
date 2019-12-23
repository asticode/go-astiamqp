package astiamqp

import (
	"github.com/asticode/go-astilog"
	"github.com/pkg/errors"
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
}

// Ack implements the Acknowledger interface
func (a acknowledger) Ack(multiple bool) (err error) {
	astilog.Debugf("astiamqp: ack %v with multiple %v", a.deliveryTag, multiple)
	if err = a.acknowledger.Ack(a.deliveryTag, multiple); err != nil {
		err = errors.Wrapf(err, "astiamqp: ack %v with multiple %v failed", a.deliveryTag, multiple)
		return
	}
	return
}

// Nack implements the Acknowledger interface
func (a acknowledger) Nack(multiple bool, requeue bool) (err error) {
	astilog.Debugf("astiamqp: nack %v with multiple %v and requeue %v", a.deliveryTag, multiple, requeue)
	if err = a.acknowledger.Nack(a.deliveryTag, multiple, requeue); err != nil {
		err = errors.Wrapf(err, "astiamqp: ack %v with multiple %v and requeue %v failed", a.deliveryTag, multiple, requeue)
		return
	}
	return
}

// Reject implements the Acknowledger interface
func (a acknowledger) Reject(requeue bool) (err error) {
	astilog.Debugf("astiamqp: reject %v with requeue %v", a.deliveryTag, requeue)
	if err = a.acknowledger.Reject(a.deliveryTag, requeue); err != nil {
		err = errors.Wrapf(err, "astiamqp: reject %v with requeue %v failed", a.deliveryTag, requeue)
		return
	}
	return
}
