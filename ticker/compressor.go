package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"ticker/producer"
	"time"
)

type Tick struct {
	Coin string  `json:"coin"` // Changed to exported field (capitalized)
	Low  float64 `json:"low"`
	High float64 `json:"high"`
}

func Compressor(duration time.Duration, in chan *producer.Tick, db *sync.Map, wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		defer wg.Done() // Ensure the WaitGroup counter is decremented when this goroutine completes

		ticker := time.NewTicker(duration)
		defer ticker.Stop() // Stop the ticker when the function completes

		ticks := make(map[string]*Tick)

		for {
			select {
			case <-ctx.Done():
				log.Println("Compressor stopped: context done")
				return
			case <-ticker.C:
				// Marshal and send tick data for each coin
				for coin, tick := range ticks {
					data, err := json.Marshal(tick)
					if err != nil {
						log.Printf("Failed to marshal tick for coin %s: %v", coin, err) // Log any marshaling errors
						continue
					}

					in <- &producer.Tick{
						Key:  coin,
						Data: data,
					}
				}
				ticks = make(map[string]*Tick) // Reset ticks after processing
			default:
				// Process current prices from the database
				db.Range(func(key, value any) bool {
					coin, ok := key.(string)
					if !ok {
						return true
					}
					price, ok := value.(float64)
					if !ok {
						return true
					}

					tick, exists := ticks[coin]
					if !exists {
						ticks[coin] = &Tick{
							Coin: coin, // Use the exported field
							Low:  price,
							High: price,
						}
						return true
					}
					// Update low and high prices
					if tick.Low > price {
						tick.Low = price
					}
					if tick.High < price {
						tick.High = price
					}
					return true
				})
			}
		}
	}()
}
