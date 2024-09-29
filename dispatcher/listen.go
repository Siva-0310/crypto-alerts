package main

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// CreateChannel creates a channel and declares a queue
func CreateChannel(conn *amqp.Connection, queue string) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	_, err = ch.QueueDeclare(
		queue, // Queue name
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	return ch, err
}

func CreateDelivary(ch *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
	deliveries, err := ch.Consume(
		queue,        // Queue name
		"dispatcher", // Consumer name
		false,        // Auto-ack
		false,        // Exclusive
		false,        // No-local
		false,        // No-wait
		nil,          // Arguments
	)
	return deliveries, err
}

// listen listens to the specified RabbitMQ queue and processes messages
func listen(conn *amqp.Connection, queue string, connString string, ctx context.Context) {
	// Create a channel for consuming messages
	ch, err := CreateChannel(conn, queue)
	if err != nil {
		log.Printf("Failed to create channel: %v", err)
		return
	}
	defer ch.Close() // Ensure the channel is closed when done

	// Start consuming messages from the queue
	deliveries, err := CreateDelivary(ch, queue)
	if err != nil {
		log.Printf("Failed to register a consumer: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done(): // Handle context cancellation (e.g., graceful shutdown)
			log.Println("Context cancelled, shutting down listener.")
			return
		case unmarshaltick := <-deliveries: // Process incoming message

			if ch.IsClosed() && !conn.IsClosed() {
				ch, err = CreateChannel(conn, queue)
				if err != nil {
					log.Printf("Failed to recreate channel after reconnecting: %v", err)
					return
				}
				deliveries, err = CreateDelivary(ch, queue)
				if err != nil {
					log.Printf("Failed to register a consumer: %v", err)
					return
				}
			} else if conn.IsClosed() {
				conn, err = CreateRabbitConn(connString)
				if err != nil {
					log.Fatalf("Unable to reconnect to RabbitMQ after 5 attempts: %v", err)
					return
				}
				ch, err = CreateChannel(conn, queue)
				if err != nil {
					log.Printf("Failed to recreate channel after reconnecting: %v", err)
					return
				}
				deliveries, err = CreateDelivary(ch, queue)
				if err != nil {
					log.Printf("Failed to register a consumer: %v", err)
					return
				}
			}

			if unmarshaltick.ContentType != "application/json" {
				unmarshaltick.Nack(false, false)
				continue
			}

			var tick map[string]interface{}
			// Unmarshal the JSON message
			if err := json.Unmarshal(unmarshaltick.Body, &tick); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				unmarshaltick.Nack(false, false) // Negatively acknowledge the message without requeueing
				continue
			}

			// Log the message content
			log.Printf("Received message: %v", tick)

			// Acknowledge the message after processing
			if err := unmarshaltick.Ack(false); err != nil {
				log.Printf("Failed to acknowledge message: %v", err)
				return
			}
		}
	}
}
