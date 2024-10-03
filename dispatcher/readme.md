# Dispatcher Service

The Dispatcher service is a Go application designed to listen for cryptocurrency price ticks from RabbitMQ and determine which alerts should be sent to the alert queue. It efficiently processes incoming messages and handles decision-making regarding alert notifications.

## Features

- **Tick Listening:** Continuously listens for cryptocurrency price ticks from a specified RabbitMQ queue.
- **Alert Decision Making:** Evaluates incoming price ticks against predefined alert criteria to determine which alerts to dispatch.
- **RabbitMQ Integration:** Utilizes RabbitMQ for reliable message handling and processing.
- **Concurrency Support:** Processes multiple tick messages simultaneously to enhance performance.
- **Duplicate Alert Prevention:** Utilizes Redis to ensure alerts are not sent to RabbitMQ if they already exist.

## Code Overview

The service is structured into several key components:

### 1. **Main Application**

The `main.go` file initializes the application, sets up environment variables, and manages the overall flow. Key functions include:

- **`GetEnv()`**: Retrieves environment variables necessary for the service to run, including the RabbitMQ connection strings, queue names, and concurrency level.
  
- **`CreateRabbitConn(connString string)`**: Establishes a connection to RabbitMQ, retrying up to five times if the connection fails.

- **Signal Handling**: Gracefully shuts down the service upon receiving termination signals (SIGINT or SIGTERM).

### 2. **Tick Listener**

- **`Listen(queueName string, wg *sync.WaitGroup, ctx context.Context, errsig chan error)`**: Listens for incoming cryptocurrency price ticks from the specified RabbitMQ queue. It processes each tick and determines if it meets alert criteria.

### 3. **Alert Dispatcher**

- **`Dispatcher` Struct**: Manages the decision-making process for alerts. Key functions include:

  - **`ProcessTick()`**: Evaluates the incoming price tick against predefined criteria to decide whether to send an alert.

## Environment Variables

The Dispatcher service requires the following environment variables to be set:

- **`RABBIT`**: The RabbitMQ connection string (e.g., `amqp://user:password@localhost:5672/`).

- **`QUEUE`**: The name of the RabbitMQ queue from which price ticks will be consumed (e.g., `tick_queue`).

- **`ALERT_QUEUE`**: The name of the RabbitMQ queue to which alerts will be sent (e.g., `alert_queue`).

- **`CONCURRENCY`**: The number of concurrent goroutines to handle processing (e.g., `5`).
- **`REDIS`**: The Redis connection string (e.g., `redis://user:password@localhost:6379/`).

## How to Run

1. Set the required environment variables as described above.
2. Run the Dispatcher service:

   ```bash
   go run main.go
   ```

## Conclusion

The Dispatcher service provides a reliable solution for processing cryptocurrency price ticks and determining alert notifications. 