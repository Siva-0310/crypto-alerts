# Architecture Overview

The architecture of the Cryptocurrency Alert System consists of several key services that work together to provide a robust solution for monitoring cryptocurrency prices and sending notifications based on user-defined alerts. The services are designed to be modular, allowing for scalability, maintainability, and ease of development. Below is an overview of each service and its purpose within the system.

## 1. Ticker Service

- **Purpose**: 
  The Ticker service is responsible for monitoring real-time cryptocurrency price changes. It receives live price updates via WebSocket connections, compresses the data, and sends the processed data to RabbitMQ for further handling.
  
- **How It Works**: 
  - The service establishes a WebSocket connection to listen for price updates.
  - It collects and compresses the price data over a defined duration (e.g., 5 seconds).
  - The compressed data is then sent to a RabbitMQ queue (TickMQ) for processing by the Dispatcher service.

- **Why This Service?**: 
  - **Real-Time Data**: Utilizing WebSocket connections allows for real-time monitoring, essential for timely alerting in the cryptocurrency market.
  - **Data Compression**: By compressing the data before sending it to the queue, the service reduces bandwidth usage and improves performance, allowing for efficient processing downstream.

## 2. Dispatcher Service

- **Purpose**: 
  The Dispatcher service listens for incoming price ticks from TickMQ and evaluates them against predefined alert criteria. It determines which alerts should be sent to the alert queue (AlertMQ) based on user-defined thresholds.

- **How It Works**: 
  - It consumes price ticks from TickMQ and processes them concurrently.
  - The service checks the incoming price data against the user's alert criteria and dispatches relevant alerts to the AlertMQ.

- **Why This Service?**: 
  - **Decoupling Logic**: Separating the alert decision-making process from data ingestion allows for cleaner code and easier maintenance.
  - **Concurrency**: By processing multiple ticks simultaneously, the Dispatcher enhances performance and responsiveness, ensuring alerts are sent promptly.

## 3. Notifier Service

- **Purpose**: 
  The Notifier service is responsible for sending notifications to users based on the alerts received from the Dispatcher service. It ensures that users do not receive duplicate messages.

- **How It Works**: 
  - The service listens for alerts from the AlertMQ.
  - It simulates sending email notifications using SendGrid and manages alert caching using Redis to prevent duplicate sends.

- **Why This Service?**: 
  - **User Experience**: Providing a reliable notification system enhances the user experience by ensuring timely alerts without unnecessary duplicates.
  - **Caching**: Utilizing Redis for alert caching improves efficiency and prevents redundancy, making the notification process more streamlined.

## 4. Postgres Database

- **Purpose**: 
  The PostgreSQL database stores user data, alert configurations, and historical price data.

- **Why This Database?**: 
  - **Reliability and Performance**: PostgreSQL is known for its reliability, powerful features, and support for complex queries, making it an excellent choice for storing structured data in a transactional system.

## 5. RabbitMQ (TickMQ and AlertMQ)

- **Purpose**: 
  RabbitMQ serves as the message broker for the system, facilitating communication between the various services. 
  - TickMQ handles price tick messages from the Ticker service.
  - AlertMQ manages alert notifications dispatched by the Dispatcher service.

- **Why RabbitMQ?**: 
  - **Reliability**: RabbitMQ provides reliable message delivery and supports multiple messaging patterns, ensuring that no messages are lost even in case of service failures.
  - **Scalability**: The use of queues allows the system to scale horizontally by adding more instances of services as needed.

## 6. Redis

- **Purpose**: 
  Redis is used for caching alerts in the Notifier service to ensure users do not receive duplicate notifications.

- **Why Redis?**: 
  - **Speed**: Redis is an in-memory data store, which provides fast access times for caching operations.
  - **Simplicity**: Its simple key-value store model is ideal for caching alerts and enhances the overall performance of the notification system.

## How to Run the Project

1. **Install Docker**: 
   Follow the official [Docker installation guide](https://docs.docker.com/get-docker/) to install Docker on your machine.

2. **Clone the Repository**:
   ```bash
   git clone <your-repo-url>
   cd <your-repo-directory>
   ```
3.	Run the Project:
    Execute the following command to start all services defined in the docker-compose.yml file:
    ```bash
    docker-compose up
    ```
## Conclusion

The Cryptocurrency Alert System is designed with a modular architecture that leverages real-time data processing, reliable messaging, and efficient notification delivery. Each service plays a vital role in ensuring that users receive timely and relevant alerts while maintaining high performance and scalability.
