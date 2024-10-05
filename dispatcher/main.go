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
	TickString     string
	TickQueue      string
	AlertQueue     string
	AlertString    string
	Concurrency    int
	PostgresString string
	RedisString    string
}

func GetEnv() *Env {
	tickString, ok := os.LookupEnv("TICK")
	if !ok {
		log.Fatal("TICK environment variable is required")
	}

	tickQueue, ok := os.LookupEnv("TICKQUEUE")
	if !ok {
		log.Fatal("TICKQUEUE environment variable is required")
	}

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

	postgres, ok := os.LookupEnv("POSTGRES")
	if !ok {
		log.Fatal("POSTGRES environment variable is required")
	}

	redis, ok := os.LookupEnv("REDIS")
	if !ok {
		log.Fatalf("REDIS environment variable is required")
	}

	return &Env{
		TickString:     tickString,
		TickQueue:      tickQueue,
		AlertQueue:     alertQueue,
		AlertString:    alertString,
		Concurrency:    concurrency,
		PostgresString: postgres,
		RedisString:    redis,
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := GetEnv()

	wg := &sync.WaitGroup{}

	ext := make(chan *Alert)

	listen := NewListener("TickMQ", env.TickString, env.TickQueue)
	global := NewGlobal(1000, env.Concurrency, ext, env.PostgresString)
	pusher := NewPusher(env.AlertQueue, env.AlertString, env.Concurrency, env.RedisString, ext)

	errsig := make(chan error, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	listen.Listen(global.Do, wg, errsig, ctx)
	pusher.Start(wg, ctx)

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
