package main

import (
	"context"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Listener struct {
	Service    string
	conn       *amqp.Connection
	ConnString string
	Queue      string
	ch         *amqp.Channel
	deliveries <-chan amqp.Delivery // Corrected spelling and type
}

func NewListener(Service string, ConnString string, Queue string) *Listener {

	l := &Listener{
		Service:    Service,
		ConnString: ConnString,
		Queue:      Queue,
	}

	var err error

	l.conn, err = CreateRabbitConn(l.Service, l.ConnString)
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ connection: %v", err)
	}

	// Create a channel for consuming messages
	l.ch, err = CreateChannel(l.conn, l.Queue)
	if err != nil {
		log.Fatalf("Failed to create channel: %v", err)
	}

	l.deliveries, err = CreateDelivery(l.ch, l.Queue)
	if err != nil {
		log.Fatalf("Failed to create deliveries: %v", err)
	}

	return l
}

func (l *Listener) CheckChan() error {
	var err error

	// Check if the channel is closed, and the connection is open
	if l.ch.IsClosed() && !l.conn.IsClosed() {
		l.ch, err = CreateChannel(l.conn, l.Queue) // Use assignment, not declaration
		if err != nil {
			return err
		}

		l.deliveries, err = CreateDelivery(l.ch, l.Queue) // Use assignment, not declaration
		if err != nil {
			return err
		}
		log.Printf("Successfully recreated AlertMQ channel")
	} else if l.conn.IsClosed() {
		l.conn, err = CreateRabbitConn(l.Service, l.ConnString) // Assuming a function to create AlertMQ connection
		if err != nil {
			return err
		}
		log.Printf("Successfully reconnected to AlertMQ")
		l.ch, err = CreateChannel(l.conn, l.Queue)
		if err != nil {
			return err
		}
		l.deliveries, err = CreateDelivery(l.ch, l.Queue)
		if err != nil {
			return err
		}
		log.Printf("Successfully recreated AlertMQ channel")
	}
	return nil
}

func (l *Listener) Listen(Do func(amqp.Delivery) (bool, bool), wg *sync.WaitGroup, errsig chan error, ctx context.Context) {

	wg.Add(1)

	log.Println("listener Started")
	go func() {
		defer func() {
			log.Println("closing")
			l.Close()
			wg.Done()
			log.Println("Listener is closed")
		}()
		for {
			select {
			case <-ctx.Done(): // Handle context cancellation (e.g., graceful shutdown)
				log.Println("Context cancelled, shutting down AlertMQ listener.")
				return
			case delivery := <-l.deliveries: // Process incoming message

				err := l.CheckChan()
				if err != nil {
					errsig <- err
					continue // Continue to the next iteration if there's an error
				}

				// Call the Do function to process the delivery
				flag, requeue := Do(delivery)

				if !flag {
					// Log or handle failure to process delivery if necessary
					// Nack the message and requeue it
					if err := delivery.Nack(false, requeue); err != nil {
						log.Printf("Failed to requeue %s message with ID '%s': %v", l.Service, delivery.MessageId, err)
					}
					continue
				}

				// Acknowledge the message after processing
				if err := delivery.Ack(false); err != nil { // Acknowledge message
					log.Printf("Failed to acknowledge %s message with ID '%s': %v", l.Service, delivery.MessageId, err)
					continue
				}
			}
		}
	}()
}

func (l *Listener) Close() {
	if l.conn != nil && l.conn.IsClosed() {
		if l.ch != nil && !l.ch.IsClosed() {
			l.ch.Close()
		}
		l.conn.Close()
	}
}
