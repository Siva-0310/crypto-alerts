package listener

import (
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func CreateRabbitMQConn(maxRetries int, serviceName string, amqpURL string) (*amqp091.Connection, error) {
	var (
		conn *amqp091.Connection
		err  error
	)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn, err = amqp091.Dial(amqpURL)
		if err == nil {
			return conn, nil
		}
		log.Printf("Failed to connect to %s, attempt %d: %v\n", serviceName, attempt, err)
		time.Sleep((1 << attempt) * time.Second)
	}
	return nil, err
}
