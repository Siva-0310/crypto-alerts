package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Settings struct {
	Timeout      time.Duration
	PostgresPool *pgxpool.Pool
}

func NewSettings(env *EnvConfig) (*Settings, error) {
	postgres, err := CreatePostgresPool(env.DBMaxConns, env.GenMaxRetries, env.DBMaxIdlePeriod, env.DBHealthCheckPeriod, env.DBURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create PostgreSQL pool: %w", err)
	}
	log.Println("PostgreSQL pool created successfully")

	return &Settings{
		Timeout:      env.GenAlertTimeout,
		PostgresPool: postgres,
	}, nil
}

func (s *Settings) AlertTimeout() time.Duration {
	return s.Timeout
}

func (s *Settings) SendEmail(to, sub, body string, ctx context.Context) error {
	return nil
}

func (s *Settings) Postgres() *pgxpool.Pool {
	return s.PostgresPool
}

func (s *Settings) Close() {
	s.PostgresPool.Close()
}
