package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

type Env struct {
	AlertQueue     string
	AlertString    string
	Concurrency    int
	RedisString    string
	PostgresString string
}

func GetEnv() *Env {
	alertQueue, ok := os.LookupEnv("ALERTQUEUE")
	if !ok {
		log.Fatal("ALERTQUEUE environment variable is required")
	}

	alertString, ok := os.LookupEnv("ALERT")
	if !ok {
		log.Fatal("ALERT environment variable is required")
	}

	concurrencyStr, ok := os.LookupEnv("CONCURRENCY")
	if !ok {
		log.Fatal("CONCURRENCY environment variable is required")
	}
	concurrency, err := strconv.Atoi(concurrencyStr)
	if err != nil {
		log.Fatalf("Invalid value for CONCURRENCY environment variable: %v", err)
	}

	redis, ok := os.LookupEnv("REDIS")
	if !ok {
		log.Fatalf("REDIS environment variable is required")
	}

	postgres, ok := os.LookupEnv("POSTGRES")
	if !ok {
		log.Fatal("POSTGRES environment variable is required")
	}

	return &Env{
		AlertQueue:     alertQueue,
		AlertString:    alertString,
		Concurrency:    concurrency,
		RedisString:    redis,
		PostgresString: postgres,
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := GetEnv()
	listener := NewListener(env.AlertString, env.AlertQueue)

	global := NewGlobal(env.Concurrency, env.PostgresString, env.RedisString)
	defer global.Close()

	wg := &sync.WaitGroup{}

	errsig := make(chan error, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Starting listener...")
	err := listener.Listen(global.Do, wg, errsig, ctx)
	if err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}

	select {
	case sig := <-sigs:
		log.Printf("Received signal: %v, initiating shutdown...", sig)
	case err := <-errsig:
		log.Printf("Received error signal: %v, initiating shutdown...", err)
	}

	cancel() // Cancelling context

	log.Println("Closing global resources...")
	global.Close()

	log.Println("Waiting for all goroutines to finish...")
	wg.Wait()

	log.Println("All goroutines finished, shutting down.")
}
