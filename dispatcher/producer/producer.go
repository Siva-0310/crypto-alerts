package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
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

type byteAlert struct {
	Key  string
	data []byte
}

type Producer struct {
	ServiceName string
	Exchange    string
	Queue       string
	AmqpURL     string
	Batch       int
	Duration    time.Duration
	In          chan *Alert
	amqpPool    *AmqpPool
	wg          *sync.WaitGroup
}

type PoolConfig struct {
	ServiceName         string
	Exchange            string
	Queue               string
	AmqpURL             string
	MaxRetries          int
	MaxConnections      int
	MaxIdealConnections int
	MaxIdealDuration    time.Duration
}

func NewProducer(serviceName, exchange, queue, url string, batch int, duration time.Duration) *Producer {
	return &Producer{
		ServiceName: serviceName,
		Exchange:    exchange,
		Queue:       queue,
		AmqpURL:     url,
		Batch:       batch,
		In:          make(chan *Alert, 2*batch),
		Duration:    duration,
		wg:          &sync.WaitGroup{},
	}
}

func (p *Producer) Init(maxRetries int) error {
	conn, err := CreateRabbitMQConn(maxRetries, p.ServiceName, p.AmqpURL)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ connection: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ channel: %w", err)
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		p.Exchange, // name
		"direct",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // nowait
		nil,        // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	q, err := ch.QueueDeclare(
		p.Queue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", p.Queue, err)
	}
	if err := ch.QueueBind(q.Name, q.Name, p.Exchange, false, nil); err != nil {
		return fmt.Errorf("failed to bind queue %s: %w", p.Queue, err)
	}
	return nil
}

func (p *Producer) InitPool(poolConfig *PoolConfig) error {
	pool, err := NewPool(poolConfig)
	p.amqpPool = pool
	return err
}

func (p *Producer) Publish(alerts []*byteAlert) error {
	conn, err := p.amqpPool.Acquire(context.Background())
	if err != nil {
		return err
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer p.amqpPool.Release(conn)

		for _, alert := range alerts {
			err := conn.Publish(alert.Key, alert.data)
			if err != nil {
				log.Printf("Failed to publish alert with key %s: %v", alert.Key, err)
			}
		}
	}()
	return nil
}

func (p *Producer) Start(errsig chan error, wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(p.Duration)
		defer ticker.Stop()

		alerts := make([]*byteAlert, 0, p.Batch)

		pubFunc := func() error {
			if len(alerts) == 0 {
				return nil // No alerts to publish
			}
			err := p.Publish(alerts)
			if err != nil {
				return err
			}
			alerts = alerts[:0] // Reset alerts after publishing
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				if len(alerts) > 0 {
					if err := pubFunc(); err != nil {
						errsig <- err
						return
					}
				}
				return
			case <-ticker.C:
				if err := pubFunc(); err != nil {
					errsig <- err
					return
				}
			case alert := <-p.In:
				jsonAlert, err := json.Marshal(alert)
				if err != nil {
					log.Printf("Failed to marshal alert with ID %d: %v", alert.ID, err)
					continue
				}
				alerts = append(alerts, &byteAlert{
					Key:  alert.Coin,
					data: jsonAlert,
				})
				if len(alerts) >= p.Batch {
					if err := pubFunc(); err != nil {
						errsig <- err
						return
					}
				}
			}
		}
	}()
}
