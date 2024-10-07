package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"ticker/listener"
	"ticker/producer"
)

func main() {
	log.Println("Service started")

	// Load environment variables
	env := LoadEnv()

	// Create input channel for producer
	in := make(chan *producer.Tick, env.BufferSize)

	// Sync structures and context
	db := &sync.Map{}
	wg := &sync.WaitGroup{}
	errsig := make(chan error, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize producer
	p := producer.NewProducer(in, env.ServiceName, env.Exchange, env.AmqpURL, env.Queues...)
	if err := p.Init(env.Retries); err != nil {
		log.Fatalf("FATAL: Failed to initialize producer: %v", err)
	}
	l := listener.NewListener(env.WebsocketURL, db)
	if err := l.Init(env.Retries); err != nil {
		log.Fatalf("FATAL: Failed to initialize listener %v", err)
	}

	// Start WebSocket listener
	log.Println("WebSocket listener started")
	l.Listen(env.Retries, env.WaitDuration, wg, errsig, ctx)
	// Start RabbitMQ monitoring
	log.Println("Monitoring RabbitMQ connection")
	p.Monitor(env.WaitDuration, env.Retries, errsig, wg, ctx)

	// Start producer
	log.Println("Producer started")
	p.Start(wg, ctx)

	// Start compressor
	log.Println("Compressor started")
	Compressor(env.Duration, in, db, wg, ctx)

	// Wait for termination or error signal
	select {
	case sig := <-sigs:
		log.Printf("Received signal: %v, shutting down...", sig)
	case err := <-errsig:
		log.Printf("Error occurred: %v, shutting down...", err)
	}

	// Cancel context and wait for all goroutines to finish
	cancel()
	wg.Wait()
	log.Println("Service shutdown completed")
}
