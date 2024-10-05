package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Tick struct {
	Coin string  `json:"coin"`
	Low  float64 `json:"low"`
	High float64 `json:"high"`
}

type Global struct {
	Pool      *pgxpool.Pool
	BatchSize int
	Ext       chan *Alert
}

func (g *Global) Close() {
	g.Pool.Close()
}

func NewGlobal(batch int, con int, ext chan *Alert, postgresString string) *Global {
	pool, err := CreatePostgresPool(int32(con), postgresString)
	if err != nil {
		log.Fatalf("Failed to create Postgres connection pool: %v", err)
	}
	return &Global{
		Pool:      pool,
		BatchSize: batch,
		Ext:       ext,
	}
}

func (g *Global) Do(d amqp.Delivery) (bool, bool) {

	if d.ContentType != "application/json" {
		log.Printf("Received message with invalid content type '%s', NACKing: %s", d.ContentType, d.MessageId)
		return false, false
	}
	var tick Tick
	// Unmarshal the JSON message
	if err := json.Unmarshal(d.Body, &tick); err != nil {
		log.Printf("Failed to unmarshal TickMQ message with ID '%s': %v", d.MessageId, err)
		return false, false
	}
	Query := `
			SELECT * 
			FROM alerts 
			WHERE coin = $1 
			  AND is_triggered = FALSE 
			  AND ((is_above = TRUE AND price <= $2) 
				OR (is_above = FALSE AND price >= $3))
			  AND id > $4 
			ORDER BY id 
			LIMIT $5`
	var lastID int

	for {

		// Querying alerts with keyset pagination
		rows, err := g.Pool.Query(context.Background(), Query,
			tick.Coin, tick.High, tick.Low, lastID, g.BatchSize)

		if err != nil {
			log.Printf("Failed to query alerts for %s: %v", tick.Coin, err)
			return false, false
		}
		defer rows.Close()

		// Check if any rows were returned

		hasrows := false

		// Process alerts
		for rows.Next() {
			hasrows = true
			var alert Alert
			if err := rows.Scan(&alert.ID, &alert.Coin, &alert.CreatedAt, &alert.IsTriggered, &alert.IsAbove, &alert.Price, &alert.UserID); err != nil {
				log.Printf("Error scanning alert for %s: %v", tick.Coin, err)
			}
			g.Ext <- &alert
			lastID = alert.ID
		}

		if !hasrows {
			break
		}

	}
	return true, false

}
