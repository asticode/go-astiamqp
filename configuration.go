package astiamqp

import (
	"flag"

	"github.com/asticode/go-astikit"
	"github.com/streadway/amqp"
)

// Constants
const (
	ExchangeTypeDirect  ExchangeType = "direct"
	ExchangeTypeFanout  ExchangeType = "fanout"
	ExchangeTypeHeaders ExchangeType = "headers"
	ExchangeTypeTopic   ExchangeType = "topic"
)

// Flags
var (
	Addr     = flag.String("amqp-addr", "", "the amqp addr")
	Password = flag.String("amqp-password", "", "the amqp password")
	Username = flag.String("amqp-username", "", "the amqp username")
)

// Configuration represents the AMQP configuration
type Configuration struct {
	Addr     string            `toml:"addr"`
	Logger   astikit.StdLogger `toml:"-"`
	Password string            `toml:"password"`
	QOS      *ConfigurationQOS `toml:"qos"`
	Username string            `toml:"username"`
}

// ConfigurationQOS represents the AMQP QOS configuration
type ConfigurationQOS struct {
	Global        bool `toml:"global"`
	PrefetchCount int  `toml:"prefetch_count"`
	PrefetchSize  int  `toml:"prefetch_size"`
}

// FlagConfig returns an AMQP config based on flags
func FlagConfig() Configuration {
	return Configuration{
		Addr:     *Addr,
		Password: *Password,
		Username: *Username,
	}
}

// ConfigurationExchange represents an exchange configuration
type ConfigurationExchange struct {
	Arguments   Table
	AutoDeleted bool
	Durable     bool
	Internal    bool
	Name        string
	NoWait      bool
	Type        ExchangeType
}

// ExchangeType represents an exchange type
type ExchangeType string

// Table wraps amqp.Table
type Table amqp.Table

// ConfigurationQueue represents a queue configuration
type ConfigurationQueue struct {
	Arguments   Table
	AutoDeleted bool
	Durable     bool
	Exclusive   bool
	Name        string
	NoWait      bool
}

// ConfigurationConsumer represents a consumer configuration
type ConfigurationConsumer struct {
	Arguments  Table
	AutoAck    bool
	Exchange   ConfigurationExchange
	Exclusive  bool
	Handler    Handler
	NoLocal    bool
	NoWait     bool
	Queue      ConfigurationQueue
	RoutingKey string
}

// Handler handles a message
type Handler func(msg []byte, routingKey string, a Acknowledger) error

// ConfigurationProducer represents a producer configuration
type ConfigurationProducer struct {
	Exchange ConfigurationExchange
}
