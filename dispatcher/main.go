package main

import (
	"context"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Env struct {
	RabbitString string
	Queue        string
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

func GetEnv() *Env {
	rabbitString, ok := os.LookupEnv("RABBIT")
	if !ok {
		log.Fatal("RABBIT environment variable is required")
	}

	queue, ok := os.LookupEnv("QUEUE")
	if !ok {
		log.Fatal("QUEUE environment variable is required")
	}

	return &Env{
		RabbitString: rabbitString,
		Queue:        queue,
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

	listen(rabbitConn, env.Queue, ctx)
}
