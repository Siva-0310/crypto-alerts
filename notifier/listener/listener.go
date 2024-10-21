package listener

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type MessageHandler func(amqp091.Delivery) (bool, bool)

type Listener struct {
	Name        string
	ServiceName string
	Queue       string
	AmqpURL     string
	MaxRetries  int
	amqpConn    *amqp091.Connection
	amqpCh      *amqp091.Channel
	wg          *sync.WaitGroup
	delivery    <-chan amqp091.Delivery
}

func NewListener(maxRetries int, name, serviceName, queue, amqpURL string, ctx context.Context) (*Listener, error) {
	// Establish a connection to RabbitMQ
	amqpConn, err := createRabbitMQConn(maxRetries, serviceName, amqpURL)
	if err != nil {
		return nil, fmt.Errorf("[%s] Failed to create RabbitMQ connection: %v", serviceName, err)
	}
	log.Printf("[%s] Successfully created RabbitMQ connection", serviceName)

	// Create a new channel
	ch, err := amqpConn.Channel()
	if err != nil {
		amqpConn.Close()
		return nil, fmt.Errorf("[%s] Failed to open channel: %v", serviceName, err)
	}
	log.Printf("[%s] Successfully opened channel", serviceName)

	// Declare the queue
	_, err = ch.QueueDeclare(
		queue,
		true,  // Durable
		false, // Auto-delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		ch.Close()
		amqpConn.Close()
		return nil, fmt.Errorf("[%s] Failed to declare queue %s: %v", serviceName, queue, err)
	}
	log.Printf("[%s] Successfully declared queue %s", serviceName, queue)

	delivery, err := ch.ConsumeWithContext(
		ctx,
		queue,
		name,
		false, // Auto-acknowledge
		false, // Exclusive
		false, // No-local
		false, // No-wait
		nil,   // Arguments
	)

	if err != nil {
		ch.Close()
		amqpConn.Close()
		return nil, fmt.Errorf("[%s] Failed to start consuming messages from queue %s: %v", serviceName, queue, err)
	}
	log.Printf("[%s] Successfully started consuming messages from queue %s", serviceName, queue)

	return &Listener{
		ServiceName: serviceName,
		Queue:       queue,
		amqpConn:    amqpConn,
		AmqpURL:     amqpURL,
		MaxRetries:  maxRetries,
		amqpCh:      ch,
		Name:        name,
		delivery:    delivery,
		wg:          &sync.WaitGroup{},
	}, nil
}

func (l *Listener) consume(handler MessageHandler) {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		for delivery := range l.delivery {
			sucess, requeue := handler(delivery)
			if !sucess {
				if err := delivery.Nack(false, requeue); err != nil {
					log.Printf("[%s] Failed to nack (requeue) message: %v", l.ServiceName, err)
				}
			} else {
				if err := delivery.Ack(false); err != nil {
					log.Printf("[%s] Failed to ack message: %v", l.ServiceName, err)
				}
			}
		}
	}()
}

func (l *Listener) Start(handler MessageHandler, d time.Duration, errsig chan error, wg *sync.WaitGroup, ctx context.Context) {
	wg.Add(1)
	go func() {
		defer func() {
			l.wg.Wait()
			wg.Done()
		}()

		l.consume(handler)
		notifyChannelClose := l.amqpCh.NotifyClose(make(chan *amqp091.Error))
		notifyConnClose := l.amqpConn.NotifyClose(make(chan *amqp091.Error))

		for {
			select {
			case <-ctx.Done():
				log.Printf("[%s] Context cancelled, shutting down listener...", l.ServiceName)
				return

			case amqpErr := <-notifyConnClose:
				log.Printf("[%s] Connection closed: %v. Attempting to reconnect...", l.ServiceName, amqpErr)
				time.Sleep(d)

				// Attempt to reconnect
				amqpConn, err := createRabbitMQConn(l.MaxRetries, l.ServiceName, l.AmqpURL)
				if err != nil {
					errsig <- fmt.Errorf("[%s] failed to reconnect: %v", l.ServiceName, err)
					return
				}
				log.Printf("[%s] Successfully reconnected to RabbitMQ", l.ServiceName)

				// Create a new channel
				ch, err := amqpConn.Channel()
				if err != nil {
					errsig <- fmt.Errorf("[%s] failed to create a channel: %v", l.ServiceName, err)
					return
				}
				log.Printf("[%s] Successfully created a new channel", l.ServiceName)

				// Update the listener's connection and channel
				l.amqpConn = amqpConn
				l.amqpCh = ch

				// Set up consumer again
				delivery, err := ch.ConsumeWithContext(
					ctx,
					l.Queue,
					l.Name,
					false,
					false,
					false,
					false,
					nil,
				)
				if err != nil {
					errsig <- fmt.Errorf("[%s] failed to consume messages: %v", l.ServiceName, err)
					return
				}
				log.Printf("[%s] Successfully resumed consuming messages from queue %s", l.ServiceName, l.Queue)

				l.delivery = delivery
				notifyConnClose = amqpConn.NotifyClose(make(chan *amqp091.Error))
				notifyChannelClose = ch.NotifyClose(make(chan *amqp091.Error))
				l.consume(handler)

			case amqpErr := <-notifyChannelClose:
				log.Printf("[%s] Channel closed: %v. Restarting channel...", l.ServiceName, amqpErr)
				time.Sleep(d)

				// Attempt to reopen a channel
				ch, err := l.amqpConn.Channel()
				if err != nil {
					errsig <- fmt.Errorf("[%s] failed to reopen a channel: %v", l.ServiceName, err)
					return
				}
				log.Printf("[%s] Successfully reopened a channel", l.ServiceName)

				l.amqpCh = ch

				delivery, err := ch.ConsumeWithContext(
					ctx,
					l.Queue,
					l.Name,
					false,
					false,
					false,
					false,
					nil,
				)
				if err != nil {
					errsig <- fmt.Errorf("[%s] failed to consume messages: %v", l.ServiceName, err)
					return
				}
				log.Printf("[%s] Successfully resumed consuming messages from queue %s", l.ServiceName, l.Queue)

				l.delivery = delivery
				notifyChannelClose = ch.NotifyClose(make(chan *amqp091.Error))
				l.consume(handler)
			}
		}
	}()
}

func (l *Listener) Close() {
	if l.amqpConn != nil && !l.amqpConn.IsClosed() {
		if l.amqpCh != nil && !l.amqpCh.IsClosed() {
			l.amqpCh.Close()
		}
		l.amqpConn.Close()
	}
}
