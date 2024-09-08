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

// Pusher is responsible for calculating the price range (low-high) over a duration.
type Pusher struct {
	Duration      time.Duration
	SenderChannel chan map[string]Record
}

// PushRecords simulates sending the records to a destination (e.g., a message queue).
func PushRecords(records map[string]Record) {
	// Simulate pushing records (replace with actual implementation)
	log.Printf("Pushing records: %v", records)
}

// StartPusher starts the pusher which processes messages from SenderChannel.
func (p *Pusher) StartPusher(ctx context.Context) {
	go func() {

		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled. Processing remaining records.")
				for scrip := range p.SenderChannel {
					PushRecords(scrip)
				}
				return
			case scrips := <-p.SenderChannel:
				PushRecords(scrips)
			}
		}
	}()
}

// Start begins the compression process, calculating the low and high prices of coins over time.
func (p *Pusher) StartCompressor(coins *sync.Map, ctx context.Context) {
	go func() {
		ticker := time.NewTicker(p.Duration)
		defer ticker.Stop()

		scrips := make(map[string]Record)

		for {
			select {
			case <-ticker.C:
				p.SenderChannel <- scrips
				scrips = make(map[string]Record) // Reset after every tick
			case <-ctx.Done():
				log.Println("Context cancelled, stopping compressor")
				close(p.SenderChannel)
				return
			default:
				coins.Range(func(key, value any) bool {
					coin, ok := key.(string)
					if !ok {
						return true
					}
					price, ok := value.(float64)
					if !ok {
						return true
					}

					record, exists := scrips[coin]
					if !exists {
						scrips[coin] = Record{Low: price, High: price}
						return true
					}

					if price < record.Low {
						record.Low = price
					}
					if price > record.High {
						record.High = price
					}

					scrips[coin] = record
					return true
				})
			}
		}
	}()
}
