# Astiamqp

Wrapper on top of amqp to provide proper configuration and error handling

# Usage

```go
// Setup logger
l := log.New(log.Writer(), log.Prefix(), log.Flags())

// Create worker
w := astikit.NewWorker(astikit.WorkerOptions{Logger: l})

// Create amqp
a := astiamqp.New(c, l)
defer a.Close()

// You can create producers before and after starting amqp
p, _ := a.NewProducer(astiamqp.ConfigurationProducer{
    Exchange: astiamqp.ConfigurationExchange{
        Durable: true,
        Name:    "my-exchange",
        Type:    astiamqp.ExchangeTypeTopic,
    },
})

// You can add consumers before and after starting amqp
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

// You can publish messages before and after starting amqp.
// If you publish a message before starting amqp, it will be buffered and will be sent once 
// it's connected to the channel
p.Publish("my payload", "my.routing.key")

// Start amqp
a.Start(w)

// Stop amqp
a.Stop()
```