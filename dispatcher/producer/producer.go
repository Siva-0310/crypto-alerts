package producer

import (
	"context"
	"dispatcher/pool"
	"encoding/json"
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

type ByteAlert struct {
	Key  string
	data []byte
}

type Producer struct {
	ServiceName   string
	Exchange      string
	Queue         string
	AmqpURL       string
	Batch         int
	In            chan *Alert
	BatchDuration time.Duration
	amqpPool      *pool.AmqpPool
	wg            *sync.WaitGroup
}

func NewProducer(serviceName, exchange, queue, url string, batchSize int, batchDuration time.Duration, config *pool.Config) (*Producer, error) {
	pool, err := pool.NewAmqpPool(config)
	if err != nil {
		return nil, err
	}
	return &Producer{
		amqpPool:      pool,
		AmqpURL:       pool.AmqpURL,
		ServiceName:   serviceName,
		Exchange:      exchange,
		Queue:         queue,
		Batch:         batchSize,
		BatchDuration: batchDuration,
		In:            make(chan *Alert, 5*batchSize),
		wg:            &sync.WaitGroup{},
	}, nil
}

func (p *Producer) Init() error {
	conn, err := p.amqpPool.Acquire(context.Background())
	if err != nil {
		return err
	}
	defer conn.Release()

	if err := conn.Exchange(p.Exchange); err != nil {
		return err
	}
	if err := conn.Queue(p.Queue); err != nil {
		return err
	}
	if err := conn.Bind(p.Queue, p.Exchange, p.Queue); err != nil {
		return err
	}
	return nil
}

func (p *Producer) Publish(alerts []*ByteAlert) error {
	conn, err := p.amqpPool.Acquire(context.Background())
	if err != nil {
		return err
	}

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer conn.Release()

		for _, alert := range alerts {
			err := conn.Publish(p.Exchange, p.Queue, alert.data)
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

		ticker := time.NewTicker(p.BatchDuration)
		defer ticker.Stop()

		alerts := make([]*ByteAlert, 0, p.Batch)

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
				alerts = append(alerts, &ByteAlert{
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

func (p *Producer) Close() {
	close(p.In)
	p.wg.Wait()
	p.amqpPool.Close()
}
