package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
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

type AlertEmail struct {
	Pool        *pgxpool.Pool
	RedisClient *redis.Client
}

func (a *AlertEmail) Email(alert *Alert, ctx context.Context) {
	var email string
	err := a.Pool.QueryRow(ctx, "SELECT email FROM users WHERE id = $1", alert.UserID).Scan(&email)
	if err != nil {
		log.Printf("Failed to fetch email for user ID %s: %v", alert.UserID, err)
		return
	}

	if sendErr := SendEmail(email, alert); sendErr != nil {
		log.Printf("Failed to send email to %s for alert ID %d: %v", email, alert.ID, sendErr)
		return
	}
	_, err = a.Pool.Exec(context.Background(), "UPDATE alerts SET is_triggered = TRUE WHERE id = $1", alert.ID)
	if err != nil {
		log.Printf("Failed to update alert ID %d in database: %v", alert.ID, err)
		return
	}

	_, err = a.RedisClient.Del(context.Background(), strconv.Itoa(alert.ID)).Result()
	if err != nil {
		log.Printf("Failed to delete alert ID %d from Redis: %v", alert.ID, err)
		return
	}

	log.Printf("Successfully sent email to %s for alert ID %d", email, alert.ID)
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
func listen(conn *amqp.Connection, queue string, connString string, wg *sync.WaitGroup, a *AlertEmail, errsig chan error, ctx context.Context) {
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

				a.Email(&alert, ctx)

				// Acknowledge the message after processing
				if err := unmarshalalert.Ack(false); err != nil {
					log.Printf("Failed to acknowledge AlertMQ message with ID '%s': %v", unmarshalalert.MessageId, err)
					return
				}
			}
		}
	}()
}
