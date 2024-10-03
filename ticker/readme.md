# Ticker Service

The Ticker service is a Go application designed to monitor real-time cryptocurrency price changes. It utilizes WebSocket connections to receive live price updates and RabbitMQ to queue and send the processed data.

## Features

- **Real-time Price Monitoring:** Continuously listens for live cryptocurrency price updates via a WebSocket connection.
- **Data Compression:** Compresses the price data over a defined duration before sending it to RabbitMQ.
- **RabbitMQ Integration:** Utilizes RabbitMQ for reliable message handling and processing.
- **Concurrency Support:** Processes multiple WebSocket messages simultaneously to enhance performance.

## Code Overview

The service is structured into several key components:

### 1. **Main Application**

The `main.go` file initializes the application, sets up environment variables, and manages the overall flow. Key functions include:

- **`GetEnv()`**: Retrieves environment variables necessary for the service to run, including the RabbitMQ connection string, queue name, concurrency level, duration for data compression, and WebSocket URL.
  
- **`CreateRabbitConn(connString string)`**: Establishes a connection to RabbitMQ, retrying up to five times if the connection fails.

- **Signal Handling**: Gracefully shuts down the service upon receiving termination signals (SIGINT or SIGTERM).

### 2. **WebSocket Listener**

- **`Listen(url string, coins *sync.Map, wg *sync.WaitGroup, ctx context.Context, errsig chan error)`**: Establishes a WebSocket connection to receive real-time cryptocurrency price updates. It handles reconnections in case of network issues and processes incoming messages.

### 3. **Pusher Component**

- **`Pusher` Struct**: Manages the process of compressing price data over a defined duration and pushing it to RabbitMQ. Key functions include:
  
  - **`StartPusher()`**: Starts a goroutine that listens for incoming price records and publishes them to RabbitMQ.
  
  - **`PushRecord()`**: Marshals the price data into JSON and sends it to the specified RabbitMQ queue. It manages channel and connection to ensure messages are sent reliably.

  - **`StartCompressor()`**: Compresses the price data over the defined duration before preparing it for publishing.

## Environment Variables

The Ticker service requires the following environment variables to be set:

- **`RABBIT`**: The RabbitMQ connection string (e.g., `amqp://user:password@localhost:5672/`).
  
- **`QUEUE`**: The name of the RabbitMQ queue where price data will be published (e.g., `price_queue`).
  
- **`CONCURRENCY`**: The number of concurrent goroutines to handle processing (e.g., `5`).
  
- **`DURATION`**: The time duration for compressing price data (e.g., `1m` for one minute).
  
- **`WEBSOCKET`**: The WebSocket URL from which to receive cryptocurrency price updates (e.g., `wss://your.websocket.url`).

## How to Run

1. Set the required environment variables as described above.
2. Run the Ticker service:

   ```bash
   go run main.go
   ```
## Conclusion

The Ticker service provides a reliable solution for monitoring cryptocurrency prices in real-time and compressing the data before sending it to RabbitMQ. Its design emphasizes performance and concurrency, making it suitable for high-availability applications in the cryptocurrency domain.