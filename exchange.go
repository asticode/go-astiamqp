package astiamqp

import (
	"fmt"

	"github.com/streadway/amqp"
)

func (a *AMQP) declareExchange(c ConfigurationExchange) (err error) {
	a.l.Debugf("astiamqp: declaring exchange %s", c.Name)
	if err = a.channel.ExchangeDeclare(
		c.Name,                  // name
		string(c.Type),          // type
		c.Durable,               // durable
		c.AutoDeleted,           // auto-deleted
		c.Internal,              // internal
		c.NoWait,                // no-wait
		amqp.Table(c.Arguments), // arguments
	); err != nil {
		err = fmt.Errorf("astiamqp: declaring exchange %+v failed: %w", c, err)
		return
	}
	return
}
