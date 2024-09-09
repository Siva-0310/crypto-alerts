package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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
	Sem           chan *amqp.Channel
	Wg            sync.WaitGroup
	Concurrency   int
	RabbitConn    *amqp.Connection
	Queue         string
}

func (p *Pusher) PushRecord(coin string, record Record, ch *amqp.Channel) {
	defer func() {
		// Ensure that the WaitGroup counter is decremented
		p.Wg.Done()
		// Release the channel back to the semaphore
		p.Sem <- ch
	}()

	// Marshal the record into JSON
	marshalled, err := json.Marshal(map[string]interface{}{
		"coin": coin,
		"high": record.High,
		"low":  record.Low,
	})
	if err != nil {
		log.Printf("Failed to marshal record for coin %s: %v", coin, err)
		return
	}

	// Publish the message to the specified queue
	err = ch.Publish("", p.Queue, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        marshalled,
	})

	if err != nil {
		log.Printf("Failed to publish message for coin %s: %v", coin, err)
	}
	log.Printf("Successfully pushed message for coin %s", coin)

}

// PushRecords simulates sending the records to a destination (e.g., a message queue).
func (p *Pusher) PushRecords(records map[string]Record) {
	// Simulate pushing records (replace with actual implementation)
	for coin, record := range records {
		ch := <-p.Sem
		p.Wg.Add(1)
		go p.PushRecord(coin, record, ch)
	}
}

// StartPusher starts the pusher which processes messages from SenderChannel.
func (p *Pusher) StartPusher(wg *sync.WaitGroup, ctx context.Context) {

	p.Sem = make(chan *amqp.Channel, p.Concurrency)
	for i := 0; i < p.Concurrency; i += 1 {
		ch, err := p.RabbitConn.Channel()
		if err != nil {
			log.Fatalf("Error creating RabbitMQ channel: %v", err)
			return
		}
		_, err = ch.QueueDeclare(
			p.Queue,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			log.Fatalf("Error declaring queue: %v", err)
			return
		}
		p.Sem <- ch
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled. Processing remaining records.")
				for scrip := range p.SenderChannel {
					p.PushRecords(scrip)
				}
				p.Wg.Wait()
				return
			case scrips := <-p.SenderChannel:
				p.PushRecords(scrips)
			}
		}
	}()
}

// Start begins the compression process, calculating the low and high prices of coins over time.
func (p *Pusher) StartCompressor(coins *sync.Map, wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		ticker := time.NewTicker(p.Duration)
		defer func() {
			ticker.Stop()
			wg.Done()
		}()

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
