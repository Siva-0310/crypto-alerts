package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func CreatePostgresPool(maxConns, maxRetries int, maxIdealTime, healthCheck time.Duration, connString string) (*pgxpool.Pool, error) {
	var (
		err  error
		pool *pgxpool.Pool
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	poolConfig.MaxConns = int32(maxConns)
	poolConfig.HealthCheckPeriod = healthCheck
	poolConfig.MaxConnIdleTime = maxIdealTime

	for i := 0; i < maxRetries; i++ {
		pool, err = pgxpool.NewWithConfig(context.Background(), poolConfig)
		if err == nil && pool.Ping(context.Background()) == nil {
			return pool, nil
		}
		log.Printf("Failed to connect to Postgres, attempt %d: %v\n", i+1, err)
		time.Sleep((1 << i) * time.Second)
	}
	return nil, err
}
