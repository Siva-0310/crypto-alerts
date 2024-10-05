package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
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

type Global struct {
	RedisClient *redis.Client
	Pool        *pgxpool.Pool
}

func NewGlobal(con int, postgresString string, redisString string) *Global {
	pool, err := CreatePostgresPool(int32(con), postgresString)
	if err != nil {
		log.Fatalf("Failed to create Postgres connection pool: %v", err)
	}
	redisClient, err := CreateRedisClient(con, redisString)
	if err != nil {
		log.Fatalf("Failed to create Redis client: %v", err)
	}
	return &Global{
		RedisClient: redisClient,
		Pool:        pool,
	}
}

func (g *Global) Close() {
	g.Pool.Close()
	g.RedisClient.Close()
}

func (g *Global) Do(delivery amqp.Delivery) (bool, bool) {
	if delivery.ContentType != "application/json" {
		log.Printf("Received message with invalid content type '%s', NACKing: %s", delivery.ContentType, delivery.MessageId)
		return false, false
	}

	var alert Alert
	err := json.Unmarshal(delivery.Body, &alert)
	if err != nil {
		log.Printf("Failed to unmarshal message body for ID '%s': %v", delivery.MessageId, err)
		return false, false
	}

	// Fetch the user's email from the database
	var email string
	err = g.Pool.QueryRow(context.Background(), "SELECT email FROM users WHERE id = $1", alert.UserID).Scan(&email)
	if err != nil {
		log.Printf("Failed to fetch email for user ID '%s': %v", alert.UserID, err)
		return false, true
	}

	// Send the alert email to the user
	SendEmail(email, &alert)

	// Update the alert in the database to set is_triggered to false
	_, err = g.Pool.Exec(context.Background(), "UPDATE alerts SET is_triggered = TRUE WHERE id = $1", alert.ID)
	if err != nil {
		log.Printf("Failed to update alert status for ID '%d': %v", alert.ID, err)
	}

	// Delete the alert from Redis
	_, err = g.RedisClient.Del(context.Background(), strconv.Itoa(alert.ID)).Result()
	if err != nil {
		log.Printf("Failed to delete alert ID '%d' from Redis: %v", alert.ID, err)
	}
	return true, false
}
