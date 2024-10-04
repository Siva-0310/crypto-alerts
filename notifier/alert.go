package main

import (
	"encoding/json"
	"log"
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

func Do(delivery amqp.Delivery) bool {
	if delivery.ContentType != "application/json" {
		log.Printf("Received message with invalid content type '%s', NACKing: %s", delivery.ContentType, delivery.MessageId)
		return false
	}

	var alert Alert
	err := json.Unmarshal(delivery.Body, &alert)
	if err != nil {
		log.Printf("Failed to unmarshal message body for ID '%s': %v", delivery.MessageId, err)
		return false
	}
	log.Println(alert)
	return true
}
