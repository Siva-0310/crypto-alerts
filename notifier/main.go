package main

import (
	"context"
	"log"
	"notifier/alerts"
	"notifier/listener"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env, err := LoadEnv()
	if err != nil {
		log.Fatal(err)
	}

	settings, err := NewSettings(env)
	if err != nil {
		log.Fatal(err)
	}

	listener, err := listener.NewListener(env.GenMaxRetries, env.MQListenerName, env.GenServiceName, env.MQQueueName, env.MQURL, ctx)
	if err != nil {
		log.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	errsig := make(chan error, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	log.Println("Starting listener...")
	listener.Start(alerts.AlertHandler(settings), env.GenAlertTimeout, errsig, wg, ctx)

	select {
	case sig := <-sigs:
		log.Printf("Received signal: %v, initiating shutdown...", sig)
	case err := <-errsig:
		log.Printf("Received error signal: %v, initiating shutdown...", err)
	}

	cancel() // Cancelling context

	log.Println("Waiting for all goroutines to finish...")
	wg.Wait()

	listener.Close()
	settings.Close()

	log.Println("All goroutines finished, shutting down.")
}
