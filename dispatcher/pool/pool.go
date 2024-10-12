package pool

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type Config struct {
	ServiceName         string
	AmqpURL             string
	MinIdealConnections int
	MaxConnections      int
	MaxRetries          int
	MaxIdealDuration    time.Duration
}

type AmqpPool struct {
	Config

	total    int
	conns    *CircularQueue[*Connection]
	sem      *semaphore.Weighted
	mu       *sync.Mutex
	wg       *sync.WaitGroup
	mChan    chan struct{}
	isClosed bool
}

func NewAmqpPool(config *Config) (*AmqpPool, error) {
	pool := &AmqpPool{
		Config:   *config,
		total:    0,
		conns:    NewCircularQueue[*Connection](config.MaxConnections),
		sem:      semaphore.NewWeighted(int64(config.MaxConnections)),
		mu:       &sync.Mutex{},
		wg:       &sync.WaitGroup{},
		mChan:    make(chan struct{}, 1),
		isClosed: false,
	}
	for i := 0; i < config.MinIdealConnections; i++ {
		conn, err := NewConnection(pool)
		if err != nil {
			return nil, fmt.Errorf("failed to create a new connection: %w", err)
		}
		pool.conns.Enqueue(conn)
	}

	pool.monitor()
	return pool, nil
}

func (p *AmqpPool) monitor() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()

		ticker := time.NewTicker(p.MaxIdealDuration)
		defer ticker.Stop()

		for {
			select {
			case <-p.mChan:
				return
			case <-ticker.C:
				p.mu.Lock()
				size := p.conns.Size()

				for i := 0; i < size; i++ {
					conn, err := p.conns.Dequeue()
					if err != nil {
						log.Printf("Error dequeuing connection: %v", err)
						break
					}
					if (conn.IsExpired() && p.total > p.MinIdealConnections) || conn.IsClosed() {
						p.total--
						conn.Close()

					} else {
						if err := p.conns.Enqueue(conn); err != nil {
							log.Printf("Error enqueuing connection: %v", err)
						}
					}
				}
				p.mu.Unlock()
			}
		}
	}()
}

func (p *AmqpPool) Acquire(ctx context.Context) (*Connection, error) {

	if err := p.sem.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("failed to acquire semaphore: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed {
		p.sem.Release(1)
		return nil, fmt.Errorf("amqp pool is closed; cannot acquire new connections")
	}

	conn, err := p.conns.Dequeue()
	if err == nil && !conn.IsClosed() {
		p.wg.Add(1)
		return conn, nil
	}

	if conn != nil && conn.IsClosed() {
		p.wg.Done()
		p.total--
	}

	conn, err = NewConnection(p)
	if err != nil {
		p.sem.Release(1)
		return nil, fmt.Errorf("failed to create a new connection: %w", err)
	}
	p.total++
	p.wg.Add(1)
	return conn, nil
}

func (p *AmqpPool) release(conn *Connection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.sem.Release(1)
	defer p.wg.Done()

	if conn.IsClosed() || conn.IsExpired() {
		p.total--
		return
	}
	p.conns.Enqueue(conn)
}

func (p *AmqpPool) Close() {
	// Signal to the monitoring goroutine to stop
	p.mChan <- struct{}{}
	p.wg.Wait() // Wait for the monitoring goroutine to finish.

	// Close remaining connections
	p.mu.Lock()         // Lock to safely access shared resources.
	defer p.mu.Unlock() // Ensure the mutex is released.

	// Dequeue and close connections

	size := p.conns.Size()
	for i := 0; i < size; i++ {
		conn, err := p.conns.Dequeue()
		if err != nil {
			log.Printf("Error dequeuing connection: %v", err)
			break // Exit if there are no more connections to close.
		}
		conn.Close()
	}

	close(p.mChan)
	p.isClosed = true // Mark the pool as closed.
}
