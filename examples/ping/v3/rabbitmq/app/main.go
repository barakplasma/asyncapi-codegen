//go:generate go run ../../../../../cmd/asyncapi-codegen -g application,types -p main -i ../../asyncapi.yaml -o ./app.gen.go

package main

import (
	"context"

	"github.com/lerenn/asyncapi-codegen/examples"
	"github.com/lerenn/asyncapi-codegen/pkg/extensions/brokers/rabbitmq"
	"github.com/lerenn/asyncapi-codegen/pkg/extensions/loggers"
	"github.com/lerenn/asyncapi-codegen/pkg/extensions/middlewares"
	testutil "github.com/lerenn/asyncapi-codegen/pkg/utils/test"
)

var _ AppSubscriber = (*Subscriber)(nil)

type Subscriber struct {
	Controller *AppController
}

func (s Subscriber) PingRequestOperationReceived(ctx context.Context, ping PingMessage) error {
	// Publish the pong message, with the callback function to modify it
	// Note: it will indefinitely wait to publish as context has no timeout
	err := s.Controller.ReplyToPingRequestOperation(ctx, ping, func(pong *PongMessage) {
		// Reply with the same event than the ping
		pong.Payload.Event = ping.Payload.Event
	})

	// Error management
	if err != nil {
		panic(err)
	}

	return nil
}

func main() {
	// Get broker address based on the environment, it will returns an address like "RabbitMQ://RabbitMQ:4222"
	// Note: this is not needed in your application, you can directly use the address
	addr := testutil.BrokerAddress(testutil.BrokerAddressParams{
		Schema:         "amqp",
		DockerizedAddr: "rabbitmq",
		Port:           "5672",
	})

	// Instantiate a RabbitMQ controller with a logger
	logger := loggers.NewText()
	broker, err := rabbitmq.NewController(
		addr,                                 // Set URL to broker
		rabbitmq.WithLogger(logger),          // Attach an internal logger
		rabbitmq.WithQueueGroup("ping-apps"), // Set a specific queue group to avoid collisions
	)
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	// Create a new app controller
	ctrl, err := NewAppController(
		broker,             // Attach the RabbitMQ controller
		WithLogger(logger), // Attach an internal logger
		WithMiddlewares(middlewares.Logging(logger))) // Attach a middleware to log messages
	if err != nil {
		panic(err)
	}
	defer ctrl.Close(context.Background())

	// Subscribe to all (we could also have just subscribed to the ping request operation)
	sub := Subscriber{Controller: ctrl}
	if err := ctrl.SubscribeToAllChannels(context.Background(), sub); err != nil {
		panic(err)
	}

	// Listen on port to let know that app is ready
	examples.ListenLocalPort(1234)
}
