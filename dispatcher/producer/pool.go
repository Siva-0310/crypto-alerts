package producer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/semaphore"
)

type Connection struct {
	conn     *amqp091.Connection
	ch       *amqp091.Channel
	amqpPool *AmqpPool
	lastUsed time.Time
}

func NewConnection(amqpPool *AmqpPool) (*Connection, error) {
	conn, err := CreateRabbitMQConn(amqpPool.MaxRetries, amqpPool.ServiceName, amqpPool.AmqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create RabbitMQ connection: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close() // Ensure the connection is closed if channel creation fails
		return nil, fmt.Errorf("failed to create RabbitMQ channel: %w", err)
	}
	return &Connection{
		conn:     conn,
		ch:       ch,
		amqpPool: amqpPool,
		lastUsed: time.Now(),
	}, nil
}

func (c *Connection) Publish(key string, data []byte) error {
	if c.ch == nil || c.ch.IsClosed() {
		ch, err := c.conn.Channel()
		if err != nil {
			return fmt.Errorf("failed to create RabbitMQ channel: %w", err)
		}
		c.ch = ch
	}

	err := c.ch.Publish(
		c.amqpPool.Exchange, // Exchange name from the pool
		key,                 // Routing key
		false,               // Mandatory
		false,               // Immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

func (c *Connection) IsClosed() bool {
	return c.conn == nil || c.conn.IsClosed()
}

func (c *Connection) IsValid() bool {
	return time.Since(c.lastUsed) < c.amqpPool.MaxIdealDuration && c.conn != nil && !c.conn.IsClosed()
}

func (c *Connection) Close() {
	if c.conn != nil && !c.conn.IsClosed() {
		if c.ch != nil && !c.ch.IsClosed() {
			if err := c.ch.Close(); err != nil {
				log.Printf("failed to close RabbitMQ channel: %v", err)
			}
		}
		if err := c.conn.Close(); err != nil {
			log.Printf("failed to close RabbitMQ connection: %v", err)
		}
	}
}

type AmqpPool struct {
	PoolConfig
	isClosed bool
	total    int
	mu       *sync.Mutex
	sem      *semaphore.Weighted
	conns    *CircularQueue[*Connection]
}

func NewPool(config *PoolConfig) (*AmqpPool, error) {
	pool := &AmqpPool{
		PoolConfig: *config,
		isClosed:   false,
		total:      0,
		mu:         &sync.Mutex{},
		sem:        semaphore.NewWeighted(int64(config.MaxConnections)),
		conns:      NewCircularQueue[*Connection](config.MaxConnections),
	}
	for i := 0; i < config.MaxIdealConnections; i++ {
		conn, err := NewConnection(pool)
		if err != nil {
			return nil, err
		}
		pool.conns.Enqueue(conn)
		pool.total++
	}
	return pool, nil
}

func (p *AmqpPool) Acquire(ctx context.Context) (*Connection, error) {
	if !p.sem.TryAcquire(1) {
		err := p.sem.Acquire(ctx, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire semaphore: %w", err)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed {
		p.sem.Release(1)
		return nil, errors.New("connection pool is closed")
	}

	conn, err := p.conns.Dequeue()
	if err == nil && !conn.IsClosed() {
		return conn, nil
	}

	if !conn.IsClosed() {
		p.total--
	}

	conn, err = NewConnection(p)
	if err != nil {
		p.sem.Release(1)
		return nil, fmt.Errorf("failed to create new connection: %w", err)
	}

	p.total++
	return conn, nil
}

func (p *AmqpPool) Release(conn *Connection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.sem.Release(1)

	if !conn.IsValid() {
		conn.Close()
		p.total--
		return
	}

	p.conns.Enqueue(conn)
}

func (p *AmqpPool) Monitor(wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(p.MaxIdealDuration)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.mu.Lock()

				remaining := p.conns.count - p.MaxIdealConnections
				for i := 0; i < remaining; i++ {
					conn, err := p.conns.Dequeue()
					if err != nil {
						log.Printf("No more connections to dequeue; remaining connections: %d", remaining)
						break // Break if there are no more connections to dequeue
					}

					if conn.IsValid() {
						// If the connection is valid, re-enqueue it back into the pool
						p.conns.Enqueue(conn)
					} else {
						// If the connection is invalid, close it and decrement the total count
						conn.Close()
						p.total--
						log.Printf("Invalid connection closed; total connections: %d", p.total)
					}
				}

				p.mu.Unlock()
			}
		}
	}()
}

func (p *AmqpPool) Close(ctx context.Context) error {
	err := p.sem.Acquire(ctx, int64(p.total))
	if err != nil {
		return fmt.Errorf("failed to acquire all semaphores during pool close: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.total; i++ {
		conn, err := p.conns.Dequeue()
		if err != nil {
			continue
		}
		conn.Close()
	}
	p.sem.Release(int64(p.total))
	return nil
}
