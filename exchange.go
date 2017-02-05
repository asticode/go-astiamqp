package astiamqp

import (
	"github.com/asticode/go-astilog"
	"github.com/pkg/errors"
)

func (a *AMQP) declareExchange(c ConfigurationExchange) (err error) {
	astilog.Debugf("astiamqp: declaring exchange %s", c.Name)
	if err = a.channel.ExchangeDeclare(
		c.Name,         // name
		string(c.Type), // type
		c.Durable,      // durable
		c.AutoDeleted,  // auto-deleted
		c.Internal,     // internal
		c.NoWait,       // no-wait
		c.Arguments,    // arguments
	); err != nil {
		err = errors.Wrapf(err, "astiamqp: declaring exchange %+v failed", c)
		return
	}
	return
}
