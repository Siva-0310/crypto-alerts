package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
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

func CreateDelivery(ch *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
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

func CreateRabbitConn(connString string) (*amqp.Connection, error) {
	var (
		err  error
		conn *amqp.Connection
	)

	for i := 0; i < 5; i++ {
		// Use TickMQ's connection function instead of RabbitMQ's
		conn, err = amqp.Dial(connString)
		if err == nil {
			return conn, nil
		}
		log.Printf("Failed to connect to AlertMQ, attempt %d: %v\n", i+1, err)
		time.Sleep((2 << i) * time.Second)
	}
	return nil, err
}

func CreatePostgresPool(con int32, connString string) (*pgxpool.Pool, error) {
	var (
		err  error
		pool *pgxpool.Pool
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	poolConfig.MaxConns = con
	poolConfig.MinConns = con / 2
	poolConfig.HealthCheckPeriod = time.Duration(15 * time.Minute)

	for i := 0; i < 5; i++ {
		pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err == nil && pool.Ping(context.Background()) == nil {
			return pool, nil
		}
		log.Printf("Failed to connect to Postgres, attempt %d: %v\n", i+1, err)
		time.Sleep((2 << i) * time.Second)
	}
	return nil, err
}

func CreateRedisClient(con int, connString string) (*redis.Client, error) {
	var (
		err  error
		conn *redis.Client
	)

	opt, err := redis.ParseURL(connString)
	opt.PoolSize = 10

	if err != nil {
		return nil, err
	}

	conn = redis.NewClient(opt)

	for i := 0; i < 5; i++ {
		_, err = conn.Ping(context.Background()).Result() // Ping the Redis server
		if err == nil {
			return conn, nil
		}
		log.Printf("Failed to connect to Redis, attempt %d: %v\n", i+1, err)
		time.Sleep((2 << i) * time.Second) // Exponential backoff
	}

	return nil, err // Return the last error after all attempts
}

func SendEmail(email string, alert *Alert) error {
	log.Printf("Simulating sending email to: %s for alert ID: %d with price: %.2f", email, alert.ID, alert.Price)
	return nil
}
