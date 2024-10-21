package listener

import (
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func createRabbitMQConn(maxRetries int, serviceName, amqpURL string) (*amqp091.Connection, error) {
	var (
		amqpConn *amqp091.Connection
		err      error
	)
	for i := 0; i < maxRetries; i++ {
		amqpConn, err = amqp091.Dial(amqpURL)
		if err == nil {
			return amqpConn, nil
		}

		log.Printf("[%s] Failed to connect to RabbitMQ on attempt %d: %v", serviceName, i+1, err)

		sleepDuration := time.Second * (1 << i)
		time.Sleep(sleepDuration)
	}
	return nil, err
}
