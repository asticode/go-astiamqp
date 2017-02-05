package astiamqp

import (
	"flag"

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
	Addr     string `toml:"addr"`
	Password string `toml:"password"`
	Username string `toml:"username"`
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
	Arguments   amqp.Table
	AutoDeleted bool
	Durable     bool
	Internal    bool
	Name        string
	NoWait      bool
	Type        ExchangeType
}

// ExchangeType represents an exchange type
type ExchangeType string

// ConfigurationQueue represents a queue configuration
type ConfigurationQueue struct {
	Arguments   amqp.Table
	AutoDeleted bool
	Durable     bool
	Exclusive   bool
	Name        string
	NoWait      bool
}

// ConfigurationConsumer represents a consumer configuration
type ConfigurationConsumer struct {
	Arguments  amqp.Table
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
