package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type EnvConfig struct {
	MQURL               string        // The RabbitMQ connection URL
	MQQueueName         string        // The name of the RabbitMQ queue
	MQListenerName      string        // The name of the listener/consumer
	GenMaxRetries       int           // The maximum number of retries for RabbitMQ and PostgreSQL connections
	GenServiceName      string        // The name of the service
	GenAlertTimeout     time.Duration // The alert processing timeout duration
	DBURL               string        // The PostgreSQL connection string
	DBMaxConns          int           // The maximum number of database connections in the pool
	DBHealthCheckPeriod time.Duration // The health check period for PostgreSQL connection pool
	DBMaxIdlePeriod     time.Duration // The maximum idle time period for PostgreSQL connections
}

func LoadEnv() (*EnvConfig, error) {
	// Helper function to get a required environment variable or return an error if missing.
	getEnvOrError := func(key string) (string, error) {
		if value, ok := os.LookupEnv(key); ok {
			return value, nil
		}
		return "", fmt.Errorf("environment variable %s is required but not set", key)
	}

	// Get environment variables
	mqURL, err := getEnvOrError("MQ_URL")
	if err != nil {
		return nil, err
	}
	mqQueueName, err := getEnvOrError("MQ_QUEUE_NAME")
	if err != nil {
		return nil, err
	}
	mqListenerName, err := getEnvOrError("MQ_LISTENER_NAME")
	if err != nil {
		return nil, err
	}
	genMaxRetriesStr, err := getEnvOrError("GEN_MAX_RETRIES")
	if err != nil {
		return nil, err
	}
	genMaxRetries, err := strconv.Atoi(genMaxRetriesStr)
	if err != nil {
		return nil, fmt.Errorf("invalid integer value for GEN_MAX_RETRIES: %v", err)
	}

	genServiceName, err := getEnvOrError("GEN_SERVICE_NAME")
	if err != nil {
		return nil, err
	}
	genAlertTimeoutStr, err := getEnvOrError("GEN_ALERT_TIMEOUT")
	if err != nil {
		return nil, err
	}
	genAlertTimeout, err := time.ParseDuration(genAlertTimeoutStr)
	if err != nil {
		return nil, fmt.Errorf("invalid value for GEN_ALERT_TIMEOUT: %v", err)
	}

	dbURL, err := getEnvOrError("DB_URL")
	if err != nil {
		return nil, err
	}
	dbMaxConnsStr, err := getEnvOrError("DB_MAX_CONNS")
	if err != nil {
		return nil, err
	}
	dbMaxConns, err := strconv.Atoi(dbMaxConnsStr)
	if err != nil {
		return nil, fmt.Errorf("invalid integer value for DB_MAX_CONNS: %v", err)
	}
	dbHealthCheckPeriodStr, err := getEnvOrError("DB_HEALTH_CHECK_PERIOD")
	if err != nil {
		return nil, err
	}
	dbHealthCheckPeriod, err := time.ParseDuration(dbHealthCheckPeriodStr)
	if err != nil {
		return nil, fmt.Errorf("invalid value for DB_HEALTH_CHECK_PERIOD: %v", err)
	}
	dbMaxIdlePeriodStr, err := getEnvOrError("DB_MAX_IDLE_PERIOD")
	if err != nil {
		return nil, err
	}
	dbMaxIdlePeriod, err := time.ParseDuration(dbMaxIdlePeriodStr)
	if err != nil {
		return nil, fmt.Errorf("invalid value for DB_MAX_IDLE_PERIOD: %v", err)
	}

	return &EnvConfig{
		MQURL:               mqURL,
		MQQueueName:         mqQueueName,
		MQListenerName:      mqListenerName,
		GenMaxRetries:       genMaxRetries,
		GenServiceName:      genServiceName,
		GenAlertTimeout:     genAlertTimeout,
		DBURL:               dbURL,
		DBMaxConns:          dbMaxConns,
		DBHealthCheckPeriod: dbHealthCheckPeriod,
		DBMaxIdlePeriod:     dbMaxIdlePeriod,
	}, nil
}
