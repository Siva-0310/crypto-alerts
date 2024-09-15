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

// listen listens to the specified RabbitMQ queue and processes messages
func listen(conn *amqp.Connection, queue string, ctx context.Context) {
	// Create a channel for consuming messages
	ch, err := CreateChannel(conn, queue)
	if err != nil {
		log.Printf("Failed to create channel: %v", err)
		return
	}
	defer ch.Close() // Ensure the channel is closed when done

	// Start consuming messages from the queue
	deliveries, err := ch.Consume(
		queue,        // Queue name
		"dispatcher", // Consumer name
		false,        // Auto-ack
		false,        // Exclusive
		false,        // No-local
		false,        // No-wait
		nil,          // Arguments
	)
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
			if unmarshaltick.ContentType != "application/json" {
				log.Printf("Skipping non-JSON message with ContentType: %s", unmarshaltick.ContentType)
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
