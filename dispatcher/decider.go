package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	Concurrency int
	Pool        *pgxpool.Pool
	Sem         chan struct{}
	Wait        *sync.WaitGroup
	In          chan map[string]interface{}
	Ext         chan *Alert
}

func (d *Decider) Coroutine(tick map[string]interface{}, ctx context.Context) {
	defer func() {
		d.Wait.Done()
		d.Sem <- struct{}{}
	}()

	// Extracting values from tick
	coin, ok := tick["coin"].(string)
	if !ok {
		log.Println("Invalid coin value")
		return
	}
	low, ok := tick["low"].(float64)
	if !ok {
		log.Println("Invalid low value")
		return
	}
	high, ok := tick["high"].(float64)
	if !ok {
		log.Println("Invalid high value")
		return
	}

	// Keyset pagination variables
	var lastID int
	limit := 1000 // Number of alerts to fetch per batch

	for {
		// Querying the alerts with keyset pagination
		rows, err := d.Pool.Query(ctx,
			`SELECT * 
			 FROM alerts 
			 WHERE coin = $1 
			   AND is_triggered = FALSE 
			   AND (
					(is_above = TRUE AND price <= $2) 
				 OR (is_above = FALSE AND price >= $3)
			   ) 
			   AND id > $4 
			 ORDER BY id 
			 LIMIT $5`,
			coin, high, low, lastID, limit)

		if err != nil {
			log.Printf("Error querying alerts: %v", err)
			return // or handle the error as needed
		}

		// Check if any rows are returned
		if !rows.Next() {
			log.Println("No more alerts found.")
			break // Exit the loop if no alerts are found
		}

		// Process the retrieved alerts
		for rows.Next() {
			var alert Alert
			if err := rows.Scan(&alert.ID, &alert.Coin, &alert.CreatedAt, &alert.IsTriggered, &alert.IsAbove, &alert.Price, &alert.UserID); err != nil {
				log.Printf("Error scanning row: %v", err)
				return // or handle the error as needed
			}

			// Send the alert to the external channel
			// d.Ext <- &alert // Send the alert directly
			log.Printf("Sent alert ID: %d for coin: %s to external channel", alert.ID, alert.Coin)

			// Update lastID to the ID of the current alert
			lastID = alert.ID

		}

		rows.Close() // Close rows after processing
	}
}

func (d *Decider) Decide(ctx context.Context) {
	for i := 0; i < d.Concurrency; i++ {
		d.Sem <- struct{}{}
	}
	log.Println("Decider started.")
	go func() {
		defer log.Println("Decider stopped.") // Log when the goroutine exits
		for {
			select {
			case <-ctx.Done():
				d.Wait.Wait()
				log.Println("Context done signal received, exiting...")
				return
			case tick := <-d.In:
				// log.Println(tick)
				<-d.Sem
				d.Wait.Add(1)
				d.Coroutine(tick, ctx)
				// Here you can add additional processing of the tick as needed
			}
		}
	}()
}
