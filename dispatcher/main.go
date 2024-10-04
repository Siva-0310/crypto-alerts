package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
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

	alertConn, err := CreateRabbitConn("AlertMQ", env.AlertString)
	if err != nil {
		log.Fatalf("Failed to create AlertMQ connection: %v", err)
	}

	pool, err := CreatePostgresPool(int32(env.Concurrency), env.PostgresString)
	if err != nil {
		log.Fatalf("Failed to create Postgres connection pool: %v", err)
	}
	defer pool.Close()

	redisConn, err := CreateRedisClient(env.Concurrency, env.RedisString)
	if err != nil {
		log.Fatalf("Failed to create Redis connection: %v", err)
	}

	wg := &sync.WaitGroup{}
	listen := NewListener("TickMQ", env.TickString, env.TickQueue)

	errsig := make(chan error, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	decider := Decider{
		In:           make(chan map[string]interface{}),
		Ext:          make(chan *Alert),
		Sem:          make(chan *amqp.Channel, env.Concurrency),
		Wait:         &sync.WaitGroup{},
		Pool:         pool,
		Concurrency:  env.Concurrency,
		Queue:        env.AlertQueue,
		RabbitConn:   alertConn,
		RabbitString: env.AlertString,
		Mu:           &sync.Mutex{},
		RedisConn:    redisConn,
	}

	decider.Decide(wg, ctx)

	listen.Listen(Do(decider.In), wg, errsig, ctx)

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
