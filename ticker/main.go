package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Env struct {
	Duration     time.Duration
	WebsocketUrl string
	RabbitString string
	Queue        string
	Concurrency  int
}

func createRabbitConn(connString string) *amqp.Connection {
	var err error
	var conn *amqp.Connection

	for i := 0; i < 5; i++ {
		conn, err = amqp.Dial(connString)
		if err == nil {
			return conn
		}
		log.Printf("Failed to connect to RabbitMQ, attempt %d: %v\n", i+1, err)
		time.Sleep(5 * time.Second)
	}

	log.Fatalf("Failed to connect to RabbitMQ after 5 attempts: %v", err)
	return nil
}

func GetEnv() *Env {
	rabbitString, ok := os.LookupEnv("RABBIT")
	if !ok {
		log.Fatal("RABBIT environment variable is required")
	}

	queue, ok := os.LookupEnv("QUEUE")
	if !ok {
		log.Fatal("QUEUE environment variable is required")
	}

	concurrencyStr, ok := os.LookupEnv("CONCURRENCY")
	if !ok {
		log.Fatal("CONCURRENCY environment variable is required")
	}
	concurrency, err := strconv.Atoi(concurrencyStr)
	if err != nil {
		log.Fatalf("Invalid value for CONCURRENCY environment variable: %v", err)
	}

	durationStr, ok := os.LookupEnv("DURATION")
	if !ok {
		log.Fatal("DURATION environment variable is required")
	}
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		log.Fatalf("Invalid value for DURATION environment variable: %v", err)
	}

	websocketUrl, ok := os.LookupEnv("WEBSOCKET")
	if !ok {
		log.Fatal("WEBSOCKET environment variable is required")
	}

	return &Env{
		Duration:     duration,
		WebsocketUrl: websocketUrl,
		RabbitString: rabbitString,
		Queue:        queue,
		Concurrency:  concurrency,
	}
}

func main() {
	// Initialize the context and cancel function
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := GetEnv()

	rabbitConn := createRabbitConn(env.RabbitString)

	// Initialize the sync.Map to hold coin data
	coins := &sync.Map{}
	wg := &sync.WaitGroup{}

	// Start listening to WebSocket and handle incoming data
	_, err := Listen(env.WebsocketUrl, coins, wg, ctx)
	if err != nil {
		log.Fatalf("Error starting WebSocket listener: %v", err)
	}

	// Set up signal handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Create and start the Pusher
	pusher := Pusher{
		Duration:      env.Duration,
		SenderChannel: make(chan map[string]Record),
		Wg:            sync.WaitGroup{},
		Concurrency:   env.Concurrency,
		Queue:         env.Queue,
		RabbitConn:    rabbitConn,
		Mu:            sync.Mutex{},
		RabbitString:  env.RabbitString,
	}

	log.Println("Starting compressor")
	pusher.StartCompressor(coins, wg, ctx)

	log.Println("Starting pusher")
	pusher.StartPusher(wg, ctx)

	// Wait for a signal to terminate
	sig := <-sigs
	log.Printf("Received signal: %v", sig)

	// Cancel context to stop processing
	cancel()
	log.Println("Context cancelled, shutting down")
	wg.Wait()
}
