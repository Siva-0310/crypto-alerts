package pool

import (
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func CreateRabbitMQConn(maxRetries int, serviceName string, amqpURL string) (*amqp091.Connection, error) {
	var (
		conn *amqp091.Connection
		err  error
	)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn, err = amqp091.Dial(amqpURL)
		if err == nil {
			return conn, nil
		}
		log.Printf("Failed to connect to %s, attempt %d: %v\n", serviceName, attempt, err)
		time.Sleep((1 << attempt) * time.Second)
	}
	return nil, err
}

type Connection struct {
	conn     *amqp091.Connection
	ch       *amqp091.Channel
	p        *AmqpPool
	lastUsed time.Time
}

func NewConnection(p *AmqpPool) (*Connection, error) {
	conn, err := CreateRabbitMQConn(p.MaxRetries, p.ServiceName, p.AmqpURL)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create a channel for service %s: %w", p.ServiceName, err)
	}
	return &Connection{
		conn:     conn,
		ch:       ch,
		p:        p,
		lastUsed: time.Now(),
	}, nil
}

func (c *Connection) Publish(exchange, key string, body []byte) error {
	if c.ch.IsClosed() {
		ch, err := c.conn.Channel()
		if err != nil {
			return fmt.Errorf("failed to create a new channel: %w", err)
		}
		c.ch = ch
	}

	defer func() {
		c.lastUsed = time.Now()
	}()

	err := c.ch.Publish(
		exchange,
		key,
		false,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp091.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to exchange %s with key %s: %w", exchange, key, err)
	}
	return nil
}

func (c *Connection) Release() {
	c.p.release(c)
}

func (c *Connection) IsExpired() bool {
	return time.Since(c.lastUsed) > c.p.MaxIdealDuration
}

func (c *Connection) IsClosed() bool {
	return c.conn == nil || c.conn.IsClosed()
}

func (c *Connection) Close() {
	if c.conn != nil && !c.conn.IsClosed() {
		if c.ch != nil && c.ch.IsClosed() {
			c.ch.Close()
		}
		c.conn.Close()
	}
}
