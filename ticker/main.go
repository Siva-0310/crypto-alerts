package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"ticker/producer"
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

func CreateRabbitConn(service string, connString string) (*amqp.Connection, error) {
	var (
		err  error
		conn *amqp.Connection
	)

	for i := 0; i < 5; i++ {
		// Use TickMQ's connection function instead of RabbitMQ's
		conn, err = amqp.Dial(connString)
		if err == nil {
			return conn, nil
		}
		log.Printf("Failed to connect to %s, attempt %d: %v\n", service, i+1, err)
		time.Sleep((2 << i) * time.Second)
	}
	return nil, err
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

	in := make(chan *producer.Tick)
	p := producer.NewProducer(in, "TickMQ", "coins", env.RabbitString, "bitcoin", "ethereum", "monero", "litecoin")
	err := p.Init(5)
	if err != nil {
		log.Fatal(err.Error())
	}

	// Initialize the sync.Map to hold coin data
	coins := &sync.Map{}
	wg := &sync.WaitGroup{}

	errsig := make(chan error, 1)

	// Start listening to WebSocket and handle incoming data
	_, err = Listen(env.WebsocketUrl, coins, wg, ctx, errsig)
	if err != nil {
		log.Fatalf("Error starting WebSocket listener: %v", err)
	}

	// Set up signal handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	p.Monitor(time.Minute, 5, errsig, wg, ctx)
	p.Start(wg, ctx)
	Compressor(env.Duration, in, coins, wg, ctx)

	// // Create and start the Pusher
	// pusher := Pusher{
	// 	Duration:      env.Duration,
	// 	SenderChannel: make(chan map[string]Record),
	// 	Wg:            sync.WaitGroup{},
	// 	Concurrency:   env.Concurrency,
	// 	Queue:         env.Queue,
	// 	RabbitConn:    rabbitConn,
	// 	Mu:            sync.Mutex{},
	// 	RabbitString:  env.RabbitString,
	// }

	// log.Println("Starting compressor")
	// pusher.StartCompressor(coins, wg, ctx)

	// log.Println("Starting pusher")
	// pusher.StartPusher(wg, ctx)

	// Wait for a signal to terminate
	select {
	case sig := <-sigs:
		log.Printf("Received signal: %v", sig)
	case err := <-errsig:
		log.Printf("Received error signal: %v", err)
	}

	cancel()

	log.Println("Context cancelled, shutting down")
	wg.Wait()
}
