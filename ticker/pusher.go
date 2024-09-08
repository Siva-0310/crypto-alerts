package main

import (
	"context"
	"log"
	"sync"
	"time"
)

// Record holds the low and high prices for a specific coin.
type Record struct {
	Low  float64
	High float64
}

// Compresser is responsible for calculating the price range (low-high) over a duration.
type Compresser struct {
	Duration time.Duration
}

// Start begins the compression process, calculating the low and high prices of coins over time.
func (c *Compresser) Start(coins *sync.Map, ctx context.Context) {
	go func() {
		// Set up a ticker to trigger at intervals defined by c.Duration
		ticker := time.NewTicker(c.Duration)
		defer ticker.Stop()

		// Create a map to store the low-high records for each coin
		scrips := make(map[string]Record)

		for {
			select {
			case <-ticker.C:
				// On each tick, print and reset the low-high records
				log.Println("Low-High records:", scrips)
				scrips = make(map[string]Record) // Reset after every tick
			case <-ctx.Done():
				// When the context is cancelled, stop the process
				log.Println("Context cancelled, stopping Compresser")
				return

			default:
				// Range over the sync.Map to process each coin and its price
				coins.Range(func(key, value any) bool {
					// Ensure the key is a string representing the coin name
					coin, ok := key.(string)
					if !ok {
						log.Println("Invalid key type, expected string")
						return true
					}

					// Ensure the value is a float64 representing the price
					price, ok := value.(float64)
					if !ok {
						log.Println("Invalid value type, expected float64")
						return true
					}

					// Get the current record for the coin, or initialize a new one
					record, exists := scrips[coin]
					if !exists {
						scrips[coin] = Record{Low: price, High: price}
						return true
					}

					// Update the low and high values based on the new price
					if price < record.Low {
						record.Low = price
					}
					if price > record.High {
						record.High = price
					}

					// Store the updated record in the scrips map
					scrips[coin] = record
					return true
				})
			}
		}
	}()
}
