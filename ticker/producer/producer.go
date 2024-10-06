package producer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Tick struct {
	Key  string
	Data []byte
}

type Producer struct {
	ServiceName string
	Queues      []string
	Exchange    string
	AmqpURL     string
	In          chan *Tick
	amqpConn    *amqp091.Connection
	amqpChan    *amqp091.Channel
	mu          *sync.Mutex
}

func NewProducer(in chan *Tick, serviceName string, exchange string, url string, queues ...string) *Producer {
	return &Producer{
		ServiceName: serviceName,
		Exchange:    exchange,
		AmqpURL:     url,
		In:          in,
		Queues:      queues,
		mu:          &sync.Mutex{},
	}
}

func (p *Producer) Init(maxRetries int) error {
	conn, err := CreateRabbitMQConn(maxRetries, p.ServiceName, p.AmqpURL)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ connection: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ channel: %w", err)
	}

	// Declare the exchange
	err = ch.ExchangeDeclare(
		p.Exchange,
		amqp091.ExchangeDirect,
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Declare and bind queues
	for _, queue := range p.Queues {
		q, err := ch.QueueDeclare(
			queue,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return fmt.Errorf("failed to declare queue %s: %w", queue, err)
		}
		if err := ch.QueueBind(q.Name, q.Name, p.Exchange, false, nil); err != nil {
			return fmt.Errorf("failed to bind queue %s: %w", queue, err)
		}
	}

	p.amqpConn = conn // Store the connection
	p.amqpChan = ch
	return nil
}

func (p *Producer) Monitor(duration time.Duration, maxRetries int, errsig chan error, wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		// NotifyClose channels for connection and channel
		connChan := p.amqpConn.NotifyClose(make(chan *amqp091.Error))
		chanChan := p.amqpChan.NotifyClose(make(chan *amqp091.Error))
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				log.Println("Monitoring stopped: context done") // Log when context is done
				return                                          // Exit if the context is done

			// Handle connection closure
			case err := <-connChan:
				p.mu.Lock() // Lock before modifying shared resources

				log.Printf("connection closed: %s\n", err.Reason)

				// Delay before attempting to reconnect
				time.Sleep(duration)

				// Attempt to reconnect
				conn, connerr := CreateRabbitMQConn(maxRetries, p.ServiceName, p.AmqpURL)
				if connerr != nil {
					errsig <- fmt.Errorf("failed to reconnect: %v", connerr)
					p.mu.Unlock()
					return
				}
				p.amqpConn = conn                                            // Update the connection
				connChan = p.amqpConn.NotifyClose(make(chan *amqp091.Error)) // Register for new close notifications

				// Attempt to recreate the channel
				ch, cherr := p.amqpConn.Channel()
				if cherr != nil {
					errsig <- fmt.Errorf("failed to create new channel: %v", cherr)
					p.mu.Unlock()
					return
				}
				p.amqpChan = ch // Update the channel
				chanChan = p.amqpChan.NotifyClose(make(chan *amqp091.Error))
				p.mu.Unlock() // Unlock after modifying the connection and channel

			// Handle channel closure
			case err := <-chanChan:
				p.mu.Lock()
				// Lock before modifying shared resources
				log.Printf("channel closed: %s\n", err.Reason)

				// Delay before attempting to recreate the channel
				time.Sleep(duration)

				// Attempt to recreate the channel
				ch, cherr := p.amqpConn.Channel()
				if cherr != nil {
					errsig <- fmt.Errorf("failed to create new channel: %v", cherr)
					p.mu.Unlock()
					return
				}
				p.amqpChan = ch // Update the channel
				chanChan = p.amqpChan.NotifyClose(make(chan *amqp091.Error))
				p.mu.Unlock() // Unlock after modifying the channel
			}
		}
	}()
}

func (p *Producer) Publish(tick *Tick) {
	if p.amqpChan != nil && p.amqpChan.IsClosed() {
		return
	}
	err := p.amqpChan.Publish(
		p.Exchange, // The exchange to publish to
		tick.Key,   // The routing key (queue name)
		false,      // Mandatory flag (false if we don't want to return undeliverable messages)
		false,      // Immediate flag (false for delayed delivery)
		amqp091.Publishing{
			ContentType: "application/json", // Specify the content type
			Body:        tick.Data,          // The message body
		},
	)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v\n", p.Exchange, err) // Log any publish errors
	}
}

func (p *Producer) Start(wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		defer func() {
			p.Close()
			wg.Done()
		}() // Ensure the WaitGroup counter is decremented when this goroutine completes
		for {
			select {
			case <-ctx.Done(): // Check if the context is done (cancellation signal)
				log.Println("Producer stopped: context done")
				return // Exit the goroutine if the context is done
			case tick := <-p.In:
				p.mu.Lock()
				p.Publish(tick)
				p.mu.Unlock() // Unlock after publishing
			}
		}
	}()
}

func (p *Producer) Close() {
	if p.amqpConn != nil && !p.amqpConn.IsClosed() {
		if p.amqpChan != nil && !p.amqpChan.IsClosed() {
			p.amqpChan.Close()
		}
		p.amqpConn.Close()
	}
}
