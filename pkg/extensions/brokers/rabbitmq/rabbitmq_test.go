package rabbitmq

import (
	"encoding/json" // Added import for JSON handling
	"sync"
	"testing"

	testutil "github.com/lerenn/asyncapi-codegen/pkg/utils/test"
	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

//nolint:funlen // this is only for testing
func TestValidateAckMechanism(t *testing.T) {
	// Establish a connection to the AMQP broker
	subj := "CoreRabbitmqValidateAckMechanism"
	rmqb, err := NewController(
		testutil.BrokerAddress(testutil.BrokerAddressParams{
			Schema:         "amqp",
			DockerizedAddr: "rabbitmq",
			Port:           "5672",
		}),
		WithQueueGroup(subj))
	assert.NoError(t, err, "new controller should not return error")
	defer rmqb.Close()

	t.Run("validate ack is supported in AMQP", func(t *testing.T) {
		queueName := "TestQueueAck"

		// Open a channel
		ch, err := rmqb.connection.Channel()
		assert.NoError(t, err, "should be able to open a channel")
		defer ch.Close()

		// Declare a queue
		_, err = ch.QueueDeclare(
			queueName,
			false, // durable
			true,  // auto-delete
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		assert.NoError(t, err, "should be able to declare queue")

		wg := sync.WaitGroup{}

		msgs, err := ch.Consume(
			queueName,
			"",    // consumer tag
			false, // auto-ack
			false, // exclusive
			false, // no-local (deprecated)
			false, // no-wait
			nil,   // arguments
		)
		assert.NoError(t, err, "should be able to start consuming messages")

		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range msgs {
				err := d.Ack(false)
				assert.NoError(t, err, "should be able to ack the message")
				break
			}
		}()

		err = ch.Publish(
			"",        // exchange
			queueName, // routing key (queue name)
			false,     // mandatory
			false,     // immediate
			amqp091.Publishing{
				ContentType: "text/plain",
				Body:        []byte("testmessage"),
			},
		)
		assert.NoError(t, err, "should be able to publish message")

		wg.Wait()
	})

	t.Run("validate nack is supported in AMQP", func(t *testing.T) {
		queueName := "TestQueueNack"

		// Open a channel
		ch, err := rmqb.connection.Channel()
		assert.NoError(t, err, "should be able to open a channel")
		defer ch.Close()

		// Declare a queue
		_, err = ch.QueueDeclare(
			queueName,
			false, // durable
			true,  // auto-delete
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		assert.NoError(t, err, "should be able to declare queue")

		wg := sync.WaitGroup{}

		msgs, err := ch.Consume(
			queueName,
			"",    // consumer tag
			false, // auto-ack
			false, // exclusive
			false, // no-local (deprecated)
			false, // no-wait
			nil,   // arguments
		)
		assert.NoError(t, err, "should be able to start consuming messages")

		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range msgs {
				err := d.Nack(false, false)
				assert.NoError(t, err, "should be able to nack the message")
				break
			}
		}()

		err = ch.Publish(
			"",        // exchange
			queueName, // routing key (queue name)
			false,     // mandatory
			false,     // immediate
			amqp091.Publishing{
				ContentType: "text/plain",
				Body:        []byte("testmessage"),
			},
		)
		assert.NoError(t, err, "should be able to publish message")

		wg.Wait()
	})

	t.Run("validate JSON body handling in AMQP", func(t *testing.T) {
		queueName := "TestQueueJSON"

		// Open a channel
		ch, err := rmqb.connection.Channel()
		assert.NoError(t, err, "should be able to open a channel")
		defer ch.Close()

		// Declare a queue
		_, err = ch.QueueDeclare(
			queueName,
			false, // durable
			true,  // auto-delete
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		assert.NoError(t, err, "should be able to declare queue")

		wg := sync.WaitGroup{}

		msgs, err := ch.Consume(
			queueName,
			"",    // consumer tag
			true,  // auto-ack
			false, // exclusive
			false, // no-local (deprecated)
			false, // no-wait
			nil,   // arguments
		)
		assert.NoError(t, err, "should be able to start consuming messages")

		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range msgs {
				assert.Equal(t, "application/json", d.ContentType, "ContentType should be application/json")
				var jsonBody map[string]interface{}
				err := json.Unmarshal(d.Body, &jsonBody)
				assert.NoError(t, err, "should be able to unmarshal JSON body")
				assert.Equal(t, "testvalue", jsonBody["testkey"], "JSON body should have expected content")
				break
			}
		}()

		// Publish a message with JSON body
		jsonMessage := map[string]interface{}{
			"testkey": "testvalue",
		}
		jsonBody, err := json.Marshal(jsonMessage)
		assert.NoError(t, err, "should be able to marshal JSON message")

		err = ch.Publish(
			"",        // exchange
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        jsonBody,
			},
		)
		assert.NoError(t, err, "should be able to publish message")

		wg.Wait()
	})
}
