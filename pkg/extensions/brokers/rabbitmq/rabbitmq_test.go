package rabbitmq

import (
	"sync"
	"testing"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestValidateAckMechanism(t *testing.T) {
	// Establish a connection to the AMQP broker
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	assert.NoError(t, err, "should be able to establish connection")
	defer conn.Close()

	t.Run("validate ack is supported in AMQP", func(t *testing.T) {
		queueName := "TestQueueAck"

		// Open a channel
		ch, err := conn.Channel()
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
		ch, err := conn.Channel()
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
}