package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	// Initialize the context and cancel function
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize the sync.Map to hold coin data
	coins := &sync.Map{}
	wg := &sync.WaitGroup{}

	// Start listening to WebSocket and handle incoming data
	_, err := Listen("wss://ws.coincap.io/prices?assets=bitcoin,ethereum,monero,litecoin", coins, wg, ctx)
	if err != nil {
		log.Fatalf("Error starting WebSocket listener: %v", err)
	}

	// Set up signal handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Create and start the Pusher
	pusher := Pusher{
		Duration:      5 * time.Second,
		SenderChannel: make(chan map[string]Record, 100),
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
