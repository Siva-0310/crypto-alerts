package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
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

type Pusher struct {
	RedisString  string
	ConnString   string
	ConnCurrency int
	Queue        string
	Ext          chan *Alert
	conn         *amqp091.Connection
	sem          chan *amqp091.Channel
	redisClient  *redis.Client
	wg           *sync.WaitGroup
	mu           *sync.Mutex
}

func NewPusher(Queue string, ConnString string, Concurrency int, RedisString string, Ext chan *Alert) *Pusher {
	p := &Pusher{
		ConnString:   ConnString,
		ConnCurrency: Concurrency,
		sem:          make(chan *amqp091.Channel, Concurrency),
		wg:           &sync.WaitGroup{},
		Ext:          Ext,
		mu:           &sync.Mutex{},
		Queue:        Queue,
		RedisString:  RedisString,
	}

	var err error

	p.conn, err = CreateRabbitConn("AlertMQ", p.ConnString)
	if err != nil {
		log.Fatalf("Failed to create AlertMQ connection: %v", err)
	}
	p.redisClient, err = CreateRedisClient(p.ConnCurrency, p.RedisString)
	if err != nil {
		log.Fatalf("Failed to create Redis connection: %v", err)
	}

	p.sem = make(chan *amqp091.Channel, p.ConnCurrency)
	for i := 0; i < p.ConnCurrency; i++ {
		ch, err := CreateChannel(p.conn, p.Queue)
		if err != nil {
			log.Fatalf("Error declaring queue: %v", err)
		}
		p.sem <- ch
	}

	return p
}

func (p *Pusher) Push(alert *Alert, ch *amqp091.Channel) {
	defer func() {
		p.sem <- ch
		p.wg.Done()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := p.redisClient.Get(ctx, strconv.Itoa(alert.ID)).Result()
	if err != nil && err != redis.Nil {
		log.Printf("Error fetching from Redis for alert ID %d: %v\n", alert.ID, err)
		return
	} else if err == nil {
		return
	}

	marshalled, err := json.Marshal(alert)
	if err != nil {
		log.Printf("Failed to marshal alert ID %d for %s: %v", alert.ID, alert.Coin, err)
		return
	}

	if ch.IsClosed() && !p.conn.IsClosed() {
		ch, err = CreateChannel(p.conn, p.Queue)
		if err != nil {
			log.Printf("Failed to create a new channel for coin %s: %v", alert.Coin, err)
			return
		}
	} else if p.conn.IsClosed() {
		p.mu.Lock()

		if p.conn.IsClosed() {
			conn, err := CreateRabbitConn("AlertMQ", p.ConnString)
			if err != nil {
				log.Fatalf("Unable to reconnect to AlertMQ after 5 attempts: %v", err)
			}
			p.conn = conn
		}
		p.mu.Unlock()
		ch, err = CreateChannel(p.conn, p.Queue)
		if err != nil {
			log.Printf("Failed to recreate channel after reconnecting: %v", err)
			return
		}
	}
	err = ch.Publish("", p.Queue, false, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        marshalled,
	})

	if err != nil {
		log.Printf("Failed to publish alert ID %d for %s: %v", alert.ID, alert.Coin, err)
	}

	err = p.redisClient.Set(ctx, strconv.Itoa(alert.ID), "", 0).Err()
	if err != nil {
		log.Printf("Error setting alert ID %d in Redis: %v\n", alert.ID, err)
	}
}

func (p *Pusher) Close() {
	close(p.Ext)
	for alert := range p.Ext {
		ch := <-p.sem
		p.wg.Add(1)
		p.Push(alert, ch)
	}
	close(p.sem)
	for ch := range p.sem {
		if !ch.IsClosed() {
			ch.Close()
		}
	}
	if p.conn != nil && !p.conn.IsClosed() {
		p.conn.Close()
	}
	p.redisClient.Close()
}

func (p *Pusher) Start(wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Pusher started") // Log when the pusher starts
		for {
			select {
			case <-ctx.Done():
				log.Println("Context done, closing pusher...") // Log when the context is canceled
				p.Close()
				log.Println("Pusher closed") // Log after closing the pusher
				return
			case alert := <-p.Ext:
				ch := <-p.sem
				p.wg.Add(1)
				p.Push(alert, ch)
			}
		}
	}()
}
