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

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type Env struct {
	AlertQueue     string
	AlertString    string
	Concurrency    int
	RedisString    string
	PostgresString string
}

func CreateRabbitConn(connString string) (*amqp.Connection, error) {
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
		log.Printf("Failed to connect to TickMQ, attempt %d: %v\n", i+1, err)
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
		log.Printf("Failed to connect to Postgres, attempt %d: %v\n", i+1, err)
		time.Sleep((2 << i) * time.Second)
	}
	return nil, err
}

func CreateRedisClient(con int, connString string) (*redis.Client, error) {
	var (
		err  error
		conn *redis.Client
	)

	opt, err := redis.ParseURL(connString)
	opt.PoolSize = 10

	if err != nil {
		return nil, err
	}

	conn = redis.NewClient(opt)

	for i := 0; i < 5; i++ {
		_, err = conn.Ping(context.Background()).Result() // Ping the Redis server
		if err == nil {
			return conn, nil
		}
		log.Printf("Failed to connect to Redis, attempt %d: %v\n", i+1, err)
		time.Sleep((2 << i) * time.Second) // Exponential backoff
	}

	return nil, err // Return the last error after all attempts
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

	alertConn, err := CreateRabbitConn(env.AlertString)
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

	alertEmail := AlertEmail{
		Pool:        pool,
		RedisClient: redisConn,
	}
	wg := &sync.WaitGroup{}

	errsig := make(chan error, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	listen(alertConn, env.AlertQueue, env.AlertString, wg, &alertEmail, errsig, ctx)

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
