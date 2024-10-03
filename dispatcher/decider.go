package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rabbitmq/amqp091-go"
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

type Decider struct {
	Concurrency  int
	Pool         *pgxpool.Pool
	Sem          chan *amqp091.Channel
	Wait         *sync.WaitGroup
	In           chan map[string]interface{}
	Ext          chan *Alert
	Queue        string
	RabbitConn   *amqp091.Connection
	Mu           *sync.Mutex
	RabbitString string
}

func (d *Decider) Coroutine(ch *amqp091.Channel, tick map[string]interface{}, ctx context.Context) {
	defer func() {
		d.Wait.Done()
		d.Sem <- ch
	}()

	// Extract values from the tick
	coin, ok := tick["coin"].(string)
	if !ok {
		log.Println("Invalid coin value in tick")
		return
	}
	low, ok := tick["low"].(float64)
	if !ok {
		log.Println("Invalid low value in tick")
		return
	}
	high, ok := tick["high"].(float64)
	if !ok {
		log.Println("Invalid high value in tick")
		return
	}

	// Pagination variables
	var lastID int
	const batchSize = 1000

	for {
		// Querying alerts with keyset pagination
		rows, err := d.Pool.Query(ctx, `
			SELECT * 
			FROM alerts 
			WHERE coin = $1 
			  AND is_triggered = FALSE 
			  AND ((is_above = TRUE AND price <= $2) 
				OR (is_above = FALSE AND price >= $3))
			  AND id > $4 
			ORDER BY id 
			LIMIT $5`,
			coin, high, low, lastID, batchSize)

		if err != nil {
			log.Printf("Failed to query alerts for %s: %v", coin, err)
			return
		}
		defer rows.Close()

		// Check if any rows were returned
		if !rows.Next() {
			break
		}

		// Process alerts
		for rows.Next() {
			var alert Alert
			if err := rows.Scan(&alert.ID, &alert.Coin, &alert.CreatedAt, &alert.IsTriggered, &alert.IsAbove, &alert.Price, &alert.UserID); err != nil {
				log.Printf("Error scanning alert for %s: %v", coin, err)
				return
			}

			marshalled, err := json.Marshal(alert)
			if err != nil {
				log.Printf("Failed to marshal alert ID %d for %s: %v", alert.ID, coin, err)
				return
			}

			// Handle RabbitMQ channel status and reconnections
			if ch.IsClosed() && !d.RabbitConn.IsClosed() {
				ch, err = CreateChannel(d.RabbitConn, d.Queue)
				if err != nil {
					log.Printf("Error creating new AlertMQ channel for %s: %v", coin, err)
					return
				}
				log.Printf("Successfully recreated AlertMQ channel for %s", coin)
			} else if d.RabbitConn.IsClosed() {
				d.Mu.Lock()
				if d.RabbitConn.IsClosed() {
					conn, err := CreateRabbitConn(d.RabbitString)
					if err != nil {
						log.Fatalf("Failed to reconnect to AlertMQ for %s: %v", coin, err)
					}
					log.Printf("Successfully reconnected to AlertMQ for %s", coin)
					d.RabbitConn = conn
				}
				d.Mu.Unlock()
				ch, err = CreateChannel(d.RabbitConn, d.Queue)
				if err != nil {
					log.Printf("Failed to recreate AlertMQ channel for %s: %v", coin, err)
					return
				}
				log.Printf("Successfully recreated AlertMQ channel for %s", coin)
			}

			// Publish the alert to RabbitMQ
			err = ch.Publish("", d.Queue, false, false, amqp091.Publishing{
				ContentType: "application/json",
				Body:        marshalled,
			})
			if err != nil {
				log.Printf("Failed to publish alert ID %d for %s: %v", alert.ID, coin, err)
			}

			// Update lastID to the latest processed alert
			lastID = alert.ID
		}
	}
}

func (d *Decider) Decide(wg *sync.WaitGroup, ctx context.Context) {
	// Initialize RabbitMQ channels
	for i := 0; i < d.Concurrency; i++ {
		ch, err := CreateChannel(d.RabbitConn, d.Queue)
		if err != nil {
			log.Fatalf("Error initializing AlertMQ queue %s: %v", d.Queue, err)
			return
		}
		d.Sem <- ch
	}

	wg.Add(1)
	log.Printf("Decider started with %d workers", d.Concurrency)

	go func() {
		defer func() {
			close(d.Sem)
			for ch := range d.Sem {
				ch.Close()
			}
			d.RabbitConn.Close()
			wg.Done()
			log.Println("Decider shut down complete")
		}()

		for {
			select {
			case <-ctx.Done():
				d.Wait.Wait()
				log.Println("Shutting down Decider on context cancellation")
				return
			case tick := <-d.In:
				ch := <-d.Sem
				d.Wait.Add(1)
				d.Coroutine(ch, tick, ctx)
			}
		}
	}()
}
