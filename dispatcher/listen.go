package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

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
func listen(conn *amqp.Connection, queue string, connString string, ext chan map[string]interface{}, wg *sync.WaitGroup, errsig chan error, ctx context.Context) {
	// Create a channel for consuming messages
	ch, err := CreateChannel(conn, queue)
	if err != nil {
		log.Printf("Failed to create channel for TickMQ queue '%s': %v", queue, err)
		return
	}
	defer ch.Close() // Ensure the channel is closed when done

	// Start consuming messages from the queue
	deliveries, err := CreateDelivary(ch, queue)
	if err != nil {
		log.Printf("Failed to register a consumer for TickMQ queue '%s': %v", queue, err)
		return
	}

	wg.Add(1)
	go func() {
		defer func() {
			ch.Close()
			conn.Close()
			wg.Done()
			log.Println("listener shut down complete")

		}()

		for {
			select {
			case <-ctx.Done(): // Handle context cancellation (e.g., graceful shutdown)
				log.Println("Context cancelled, shutting down TickMQ listener.")
				return
			case unmarshaltick := <-deliveries: // Process incoming message

				if ch.IsClosed() && !conn.IsClosed() {
					ch, err = CreateChannel(conn, queue)
					if err != nil {
						log.Printf("Failed to recreate channel for TickMQ queue '%s' after reconnecting: %v", queue, err)
						return
					}
					deliveries, err = CreateDelivary(ch, queue)
					if err != nil {
						log.Printf("Failed to register a consumer for TickMQ queue '%s' after recreating channel: %v", queue, err)
						return
					}
					log.Printf("Successfully recreate TickMQ channel")
				} else if conn.IsClosed() {
					conn, err = CreateRabbitConn(connString) // Assuming a function to create TickMQ connection
					if err != nil {
						log.Printf("Failed to reconnect to TickMQ: %v", err)
						errsig <- err
						return
					}
					log.Printf("Successfully reconnected to TickMQ")
					ch, err = CreateChannel(conn, queue)
					if err != nil {
						log.Printf("Failed to recreate channel for TickMQ queue '%s' after reconnecting: %v", queue, err)
						return
					}
					deliveries, err = CreateDelivary(ch, queue)
					if err != nil {
						log.Printf("Failed to register a consumer for TickMQ queue '%s' after recreating channel: %v", queue, err)
						return
					}
					log.Printf("Successfully recreate TickMQ channel")
				}

				if unmarshaltick.ContentType != "application/json" {
					log.Printf("Received message with invalid content type '%s', NACKing: %s", unmarshaltick.ContentType, unmarshaltick.MessageId)
					unmarshaltick.Nack(false, false)
					continue
				}

				var tick map[string]interface{}
				// Unmarshal the JSON message
				if err := json.Unmarshal(unmarshaltick.Body, &tick); err != nil {
					log.Printf("Failed to unmarshal TickMQ message with ID '%s': %v", unmarshaltick.MessageId, err)
					unmarshaltick.Nack(false, false) // Negatively acknowledge the message without requeueing
					continue
				}

				ext <- tick

				// Acknowledge the message after processing
				if err := unmarshaltick.Ack(false); err != nil {
					log.Printf("Failed to acknowledge TickMQ message with ID '%s': %v", unmarshaltick.MessageId, err)
					return
				}
			}
		}
	}()
}
