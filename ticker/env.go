package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Env struct {
	ServiceName  string
	Exchange     string
	WebsocketURL string
	AmqpURL      string
	Queues       []string
	Retries      int
	BufferSize   int
	Duration     time.Duration
	WaitDuration time.Duration
}

func LoadEnv() *Env {
	// Check for SERVICE
	serviceName, ok := os.LookupEnv("SERVICE")
	if !ok || serviceName == "" {
		log.Fatal("FATAL: Missing required environment variable SERVICE")
	}

	// Check for EXCHANGE
	exchange, ok := os.LookupEnv("EXCHANGE")
	if !ok || exchange == "" {
		log.Fatal("FATAL: Missing required environment variable EXCHANGE")
	}

	// Check for WEBSOCKET_URL
	websocketURL, ok := os.LookupEnv("WEBSOCKET_URL")
	if !ok || websocketURL == "" {
		log.Fatal("FATAL: Missing required environment variable WEBSOCKET_URL")
	}

	// Check for AMQP_URL
	amqpURL, ok := os.LookupEnv("AMQP_URL")
	if !ok || amqpURL == "" {
		log.Fatal("FATAL: Missing required environment variable AMQP_URL")
	}

	// Check for QUEUES (comma-separated)
	queuesStr, ok := os.LookupEnv("QUEUES")
	if !ok || queuesStr == "" {
		log.Fatal("FATAL: Missing required environment variable QUEUES")
	}
	queues := strings.Split(queuesStr, ",")

	// Check for RETRIES and convert to int
	retriesStr, ok := os.LookupEnv("RETRIES")
	if !ok || retriesStr == "" {
		log.Fatal("FATAL: Missing required environment variable RETRIES")
	}
	retries, err := strconv.Atoi(retriesStr)
	if err != nil {
		log.Fatalf("FATAL: Invalid value for RETRIES environment variable: %v", err)
	}

	// Check for BUFFER_SIZE and convert to int
	bufferSizeStr, ok := os.LookupEnv("BUFFER_SIZE")
	if !ok || bufferSizeStr == "" {
		log.Fatal("FATAL: Missing required environment variable BUFFER_SIZE")
	}
	bufferSize, err := strconv.Atoi(bufferSizeStr)
	if err != nil {
		log.Fatalf("FATAL: Invalid value for BUFFER_SIZE environment variable: %v", err)
	}

	// Check for DURATION and parse as time.Duration
	durationStr, ok := os.LookupEnv("DURATION")
	if !ok || durationStr == "" {
		log.Fatal("FATAL: Missing required environment variable DURATION")
	}
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		log.Fatalf("FATAL: Invalid value for DURATION environment variable: %v", err)
	}

	// Check for WAIT_DURATION and parse as time.Duration
	waitDurationStr, ok := os.LookupEnv("WAIT_DURATION")
	if !ok || waitDurationStr == "" {
		log.Fatal("FATAL: Missing required environment variable WAIT_DURATION")
	}
	waitDuration, err := time.ParseDuration(waitDurationStr)
	if err != nil {
		log.Fatalf("FATAL: Invalid value for WAIT_DURATION environment variable: %v", err)
	}

	// Return the populated Env struct
	return &Env{
		ServiceName:  serviceName,
		Exchange:     exchange,
		WebsocketURL: websocketURL,
		AmqpURL:      amqpURL,
		Queues:       queues,
		Retries:      retries,
		BufferSize:   bufferSize,
		Duration:     duration,
		WaitDuration: waitDuration,
	}
}
