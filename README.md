# Astiamqp

Wrapper on top of amqp to provide proper configuration and error handling

# Usage

```go
// Create
a := astiamqp.New(c)
defer a.Close()

// Init
a.Init(context.Background())

// Add producer
p, _ := a.AddProducer(astiamqp.ConfigurationProducer{
    Exchange: astiamqp.ConfigurationExchange{
        Durable: true,
        Name:    "my-exchange",
        Type:    astiamqp.ExchangeTypeTopic,
    },
})

// Publish
p.Publish("my payload", "my.routing.key")

// Add consumer
a.AddConsumer(astiamqp.ConfigurationConsumer{
    AutoAck: false,
    Exchange: astiamqp.ConfigurationExchange{
        Durable: true,
        Name:    "my-exchange",
        Type:    astiamqp.ExchangeTypeTopic,
    },
    Handler: myHandler,
    Queue: astiamqp.ConfigurationQueue{
        Durable: true,
        Name:    "my-queue",
    },
    RoutingKey: "my.routing.key",
})

// Stop
a.Stop()
```