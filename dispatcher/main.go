package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Env struct {
	RabbitString   string
	Queue          string
	Concurrency    int
	PostgresString string
}

func CreateRabbitConn(connString string) (*amqp.Connection, error) {
	var (
		err  error
		conn *amqp.Connection
	)

	for i := 0; i < 5; i++ {
		conn, err = amqp.Dial(connString)
		if err == nil {
			return conn, nil
		}
		log.Printf("Failed to connect to RabbitMQ, attempt %d: %v\n", i+1, err)
		time.Sleep((2 << i) * time.Second)
	}
	return nil, err
}
func CreatePostgresPool(con int32, connString string) (*pgxpool.Pool, error) {
	var (
		err  error
		pool *pgxpool.Pool
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	poolConfig.MaxConns = con
	poolConfig.MinConns = con / 2
	poolConfig.HealthCheckPeriod = time.Duration(15 * time.Minute)

	for i := 0; i < 5; i++ {
		pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err == nil && pool.Ping(context.Background()) == nil {
			return pool, nil
		}
		log.Printf("Failed to connect to RabbitMQ, attempt %d: %v\n", i+1, err)
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
	postgres, ok := os.LookupEnv("POSTGRES")
	if !ok {
		log.Fatal("Environment variable POSTGRES is not set")
	}

	return &Env{
		RabbitString:   rabbitString,
		Queue:          queue,
		Concurrency:    concurrency,
		PostgresString: postgres,
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := GetEnv()

	rabbitConn, err := CreateRabbitConn(env.RabbitString)
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ connection: %v", err)
	}

	pool, err := CreatePostgresPool(int32(env.Concurrency), env.PostgresString)
	if err != nil {
		log.Fatalf("Failed to create Postgres connection: %v", err)
	}

	decider := Decider{
		In:          make(chan map[string]interface{}),
		Ext:         make(chan *Alert),
		Sem:         make(chan struct{}, env.Concurrency),
		Wait:        &sync.WaitGroup{},
		Pool:        pool,
		Concurrency: env.Concurrency,
	}

	decider.Decide(ctx)

	listen(rabbitConn, env.Queue, env.RabbitString, decider.In, ctx)
}
