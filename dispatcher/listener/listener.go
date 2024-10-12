package listener

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Listener struct {
	Name        string
	ServiceName string
	Queue       string
	AmqpURL     string
	amqpConn    *amqp091.Connection
	amqpChan    *amqp091.Channel
	deliveries  <-chan amqp091.Delivery
	mu          *sync.Mutex
	re          chan struct{}
}

func NewListener(name string, url string, serviceName string, queue string) *Listener {
	return &Listener{
		Name:        name,
		ServiceName: serviceName,
		Queue:       queue,
		AmqpURL:     url,
		mu:          &sync.Mutex{},
		re:          make(chan struct{}),
	}
}

func (l *Listener) createDelivery(ch *amqp091.Channel) (<-chan amqp091.Delivery, error) {
	deliveries, err := ch.Consume(
		l.Queue, // Queue name
		l.Name,
		false, // Auto-ack
		false, // Exclusive
		false, // No-local
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare delivery for queue %s: %w", l.Queue, err)
	}

	return deliveries, nil
}

func (l *Listener) Init(maxRetries int) error {
	conn, err := CreateRabbitMQConn(maxRetries, l.ServiceName, l.AmqpURL)
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ connection: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create RabbitMQ channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		l.Queue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", l.Queue, err)
	}

	deliveries, err := l.createDelivery(ch)
	if err != nil {
		return fmt.Errorf("failed to declare deliveries for queue %s: %w", l.Queue, err)
	}

	l.amqpConn = conn         // Store the connection
	l.amqpChan = ch           // Store the channel
	l.deliveries = deliveries // Store the deliveries channel
	return nil
}

func (l *Listener) Monitor(duration time.Duration, maxRetries int, errsig chan error, wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		// NotifyClose channels for connection and channel
		connChan := l.amqpConn.NotifyClose(make(chan *amqp091.Error))
		chanChan := l.amqpChan.NotifyClose(make(chan *amqp091.Error))
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				log.Println("Monitoring stopped: context done") // Log when context is done
				return                                          // Exit if the context is done

			// Handle connection closure
			case err := <-connChan:
				l.mu.Lock() // Lock before modifying shared resources

				log.Printf("connection closed: %s\n", err.Reason)

				// Delay before attempting to reconnect
				time.Sleep(duration)

				// Attempt to reconnect
				conn, connerr := CreateRabbitMQConn(maxRetries, l.ServiceName, l.AmqpURL)
				if connerr != nil {
					errsig <- fmt.Errorf("failed to reconnect: %v", connerr)
					l.mu.Unlock()
					return
				}
				l.amqpConn = conn                                            // Update the connection
				connChan = l.amqpConn.NotifyClose(make(chan *amqp091.Error)) // Register for new close notifications

				// Attempt to recreate the channel
				ch, cherr := l.amqpConn.Channel()
				if cherr != nil {
					errsig <- fmt.Errorf("failed to create new channel: %v", cherr)
					l.mu.Unlock()
					return
				}

				deliveries, cherr := l.createDelivery(ch)
				if cherr != nil {
					errsig <- fmt.Errorf("failed to declare deliveries: %v", cherr)
					l.mu.Unlock()
					return
				}
				l.deliveries = deliveries
				l.amqpChan = ch // Update the channel
				chanChan = l.amqpChan.NotifyClose(make(chan *amqp091.Error))
				l.re <- struct{}{}
				l.mu.Unlock() // Unlock after modifying the connection and channel

			// Handle channel closure
			case err := <-chanChan:
				l.mu.Lock()
				// Lock before modifying shared resources
				log.Printf("channel closed: %s\n", err.Reason)

				// Delay before attempting to recreate the channel
				time.Sleep(duration)

				// Attempt to recreate the channel
				ch, cherr := l.amqpConn.Channel()
				if cherr != nil {
					errsig <- fmt.Errorf("failed to create new channel: %v", cherr)
					l.mu.Unlock()
					return
				}
				deliveries, cherr := l.createDelivery(ch)
				if cherr != nil {
					errsig <- fmt.Errorf("failed to declare deliveries: %v", cherr)
					l.mu.Unlock()
					return
				}
				l.deliveries = deliveries
				l.amqpChan = ch // Update the channel
				chanChan = l.amqpChan.NotifyClose(make(chan *amqp091.Error))
				l.re <- struct{}{}
				l.mu.Unlock() // Unlock after modifying the channel
			}
		}
	}()
}

func (l *Listener) consume(do func(amqp091.Delivery) (bool, bool), deliveries <-chan amqp091.Delivery, re chan struct{}, ctx context.Context) {
	go func() {
		for {
			select {
			case <-re:
				return // Exit the goroutine on restart
			case <-ctx.Done(): // Check if the context is done (cancellation signal)
				return // Exit the goroutine if the context is done
			case tick, ok := <-deliveries:
				if !ok {
					// Channel is closed; exit the loop
					return
				}

				l.mu.Lock()
				flag, requeue := do(tick)
				if !flag {
					// Nack the message and requeue it
					if err := tick.Nack(false, requeue); err != nil {
						log.Printf("Failed to requeue %s message with ID '%s': %v", l.ServiceName, tick.MessageId, err)
					}
					l.mu.Unlock() // Unlock after handling Nack
					continue
				}

				// Acknowledge the message after processing
				if err := tick.Ack(false); err != nil {
					log.Printf("Failed to acknowledge %s message with ID '%s': %v", l.ServiceName, tick.MessageId, err)
				}
				l.mu.Unlock() // Unlock after processing
			}
		}
	}()
}

func (l *Listener) Start(do func(amqp091.Delivery) (bool, bool), wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		defer func() {
			l.Close()
			wg.Done()
		}()
		re := make(chan struct{})
		log.Println("Listener started.")
		l.consume(do, l.deliveries, re, ctx)
		for {
			select {
			case <-ctx.Done():
				log.Println("Listener stopped: context done")
				return
			case <-l.re:
				log.Println("Restarting listener...")
				re <- struct{}{}
				l.consume(do, l.deliveries, re, ctx)
			}
		}
	}()
}

func (l *Listener) Close() {
	if l.amqpConn != nil && l.amqpConn.IsClosed() {
		if l.amqpChan != nil && !l.amqpChan.IsClosed() {
			l.amqpChan.Close()
		}
		l.amqpChan.Close()
	}
}
