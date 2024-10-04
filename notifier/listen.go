package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Alert struct {
	ID          int       `json:"id"`           // The primary key of the alert.
	Coin        string    `json:"coin"`         // The cryptocurrency coin associated with the alert.
	CreatedAt   time.Time `json:"created_at"`   // The timestamp when the alert was created.
	IsTriggered bool      `json:"is_triggered"` // Indicates if the alert has been triggered.
	IsAbove     bool      `json:"is_above"`     // Indicates if the alert is set for a price above or below.
	Price       float64   `json:"price"`        // The price threshold for the alert.
	UserID      string    `json:"user_id"`      // The user ID associated with the alert.
}

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
func listen(conn *amqp.Connection, queue string, connString string, ext chan *Alert, wg *sync.WaitGroup, errsig chan error, ctx context.Context) {
	// Create a channel for consuming messages
	ch, err := CreateChannel(conn, queue)
	if err != nil {
		log.Printf("Failed to create channel for AlertMQ queue '%s': %v", queue, err)
		return
	}
	defer ch.Close() // Ensure the channel is closed when done

	// Start consuming messages from the queue
	deliveries, err := CreateDelivary(ch, queue)
	if err != nil {
		log.Printf("Failed to register a consumer for AlertMQ queue '%s': %v", queue, err)
		return
	}

	wg.Add(1)
	go func() {
		defer func() {
			if conn != nil && !conn.IsClosed() {
				if !ch.IsClosed() {
					ch.Close()
				}
				conn.Close()
			}
			wg.Done()
			log.Println("listener is closed")

		}()

		for {
			select {
			case <-ctx.Done(): // Handle context cancellation (e.g., graceful shutdown)
				log.Println("Context cancelled, shutting down AlertMQ listener.")
				return
			case unmarshalalert := <-deliveries: // Process incoming message

				if ch.IsClosed() && !conn.IsClosed() {
					ch, err = CreateChannel(conn, queue)
					if err != nil {
						log.Printf("Failed to recreate channel for AlertMQ queue '%s' after reconnecting: %v", queue, err)
						return
					}
					deliveries, err = CreateDelivary(ch, queue)
					if err != nil {
						log.Printf("Failed to register a consumer for AlertMQ queue '%s' after recreating channel: %v", queue, err)
						return
					}
					log.Printf("Successfully recreate AlertMQ channel")
				} else if conn.IsClosed() {
					conn, err = CreateRabbitConn(connString) // Assuming a function to create AlertMQ connection
					if err != nil {
						log.Printf("Failed to reconnect to AlertMQ: %v", err)
						errsig <- err
						return
					}
					log.Printf("Successfully reconnected to AlertMQ")
					ch, err = CreateChannel(conn, queue)
					if err != nil {
						log.Printf("Failed to recreate channel for AlertMQ queue '%s' after reconnecting: %v", queue, err)
						return
					}
					deliveries, err = CreateDelivary(ch, queue)
					if err != nil {
						log.Printf("Failed to register a consumer for AlertMQ queue '%s' after recreating channel: %v", queue, err)
						return
					}
					log.Printf("Successfully recreate AlertMQ channel")
				}

				if unmarshalalert.ContentType != "application/json" {
					log.Printf("Received message with invalid content type '%s', NACKing: %s", unmarshalalert.ContentType, unmarshalalert.MessageId)
					unmarshalalert.Nack(false, false)
					continue
				}

				var alert Alert
				// Unmarshal the JSON message
				if err := json.Unmarshal(unmarshalalert.Body, &alert); err != nil {
					log.Printf("Failed to unmarshal AlertMQ message with ID '%s': %v", unmarshalalert.MessageId, err)
					unmarshalalert.Nack(false, false) // Negatively acknowledge the message without requeueing
					continue
				}

				ext <- &alert

				// Acknowledge the message after processing
				if err := unmarshalalert.Ack(false); err != nil {
					log.Printf("Failed to acknowledge AlertMQ message with ID '%s': %v", unmarshalalert.MessageId, err)
					return
				}
			}
		}
	}()
}
