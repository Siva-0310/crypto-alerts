# Notifier Service

The Notifier service is a Go application designed to send notifications to users based on cryptocurrency price alerts. It efficiently handles alert notifications while ensuring that users do not receive duplicate messages.

## Features

- **Multi-Channel Notifications:** Simulates sending notifications via email and other channels based on user-defined alerts.
- **Alert Caching:** Utilizes Redis to cache alerts, ensuring that notifications are not sent multiple times for the same alert.
- **Simulated Email Sending:** Integrates with SendGrid to simulate sending email notifications without actually implementing the email sending functionality.

## Code Overview

The service is structured into several key components:

### 1. **Main Application**

The `main.go` file initializes the application, sets up environment variables, and manages the overall flow. Key functions include:

- **`GetEnv()`**: Retrieves environment variables necessary for the service to run, including Redis connection strings and other configurations.

- **Signal Handling**: Gracefully shuts down the service upon receiving termination signals (SIGINT or SIGTERM).

### 2. **Alert Processing**

- **`Do(delivery amqp.Delivery) bool`**: Processes incoming alerts and determines whether to send notifications based on the alert status and user settings. 

### 3. **Simulated Email Sending**

- **`SendEmail(email string, alert *Alert) error`**: Simulates sending an email notification to the user for a specific alert.

## Environment Variables

The Notifier service requires the following environment variables to be set:

- **`ALERT`**: The alert string used for notifications.

- **`CONCURRENCY`**: The number of concurrent goroutines to handle processing (e.g., `5`).

- **`REDIS`**: The Redis connection string (e.g., `redis://user:password@localhost:6379/`).

## How to Run

1. Set the required environment variables as described above.
2. Run the Notifier service:

   ```bash
   go run main.go
   ``` 
## Conclusion

The Notifier service provides a reliable solution for processing cryptocurrency alerts and sending notifications to users. By leveraging Redis for caching and simulating email sending, the service ensures efficient and effective alert management.