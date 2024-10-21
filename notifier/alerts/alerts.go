package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"notifier/listener"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rabbitmq/amqp091-go"
)

const htmlFormat = `
		<!DOCTYPE html>
		<html lang="en">
		<head>
			<meta charset="UTF-8">
			<meta name="viewport" content="width=device-width, initial-scale=1.0">
			<title>Alert Notification</title>
			<style>
		
				body {
					font-family: Arial, sans-serif;
					margin: 0;
					padding: 20px;
					background-color: #f4f4f4;
				}
				.container {
					max-width: 600px;
					margin: auto;
					background: white;
					padding: 20px;
					border-radius: 8px;
					box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
				}
				h1 {
					color: #333;
				}
				p {
					line-height: 1.6;
				}
				.footer {
					font-size: 12px;
					color: #777;
					text-align: center;
					margin-top: 20px;
				}
			</style>
		</head>
		<body>
			<div class="container">
				<h1>Alert Notification for %s</h1>
				<p>Dear User, your alert for <strong>%s</strong> has been triggered. This occurred on <strong>%s UTC</strong>, and the current price is <strong>$%.2f</strong>.</p>
				
				<p>As a reminder, the alert was triggered because %s Please take appropriate action based on this alert. Thank you for your attention!</p>

				<p>Best Regards,<br>Your Alert System Team</p>
				
				<div class="footer">
					This is an automated message. Please do not reply to this email.
				</div>
			</div>
		</body>
		</html>
	`

type Settings interface {
	AlertTimeout() time.Duration
	SendEmail(string, string, string, context.Context) error
	Postgres() *pgxpool.Pool
}

type Alert struct {
	ID           int       `json:"id"`           // The primary key of the alert.
	Coin         string    `json:"coin"`         // The cryptocurrency coin associated with the alert.
	CreatedAt    time.Time `json:"created_at"`   // The timestamp when the alert was created.
	IsTriggered  bool      `json:"is_triggered"` // Indicates if the alert has been triggered.
	IsAbove      bool      `json:"is_above"`     // Indicates if the alert is set for a price above or below.
	Price        float64   `json:"price"`        // The price threshold for the alert.
	UserID       string    `json:"user_id"`      // The user ID associated with the alert.
	TriggeredAt  time.Time `json:"triggered_at"`
	CurrentPrice float64   `json:"curr_price"`
}

func AlertHandler(settings Settings) listener.MessageHandler {
	return func(d amqp091.Delivery) (bool, bool) {
		// Check if the message content type is JSON
		if d.ContentType != "application/json" {
			log.Println("Invalid content type. Expected 'application/json'.")
			return false, false
		}

		ctx, cancel := context.WithTimeout(context.Background(), settings.AlertTimeout())
		defer cancel()

		var alert Alert

		// Unmarshal the message body into the Alert struct
		err := json.Unmarshal(d.Body, &alert)
		if err != nil {
			log.Println("Error unmarshaling message body:", err)
			return false, true
		}

		var email string
		postgres := settings.Postgres()

		// Query the email associated with the user ID from the alert
		err = postgres.QueryRow(ctx, "SELECT email FROM users WHERE user_id = $1", alert.UserID).Scan(&email)
		if err != nil {
			log.Println("Error querying email from PostgreSQL:", err)
			return false, true
		}

		tx, err := postgres.Begin(ctx)
		if err != nil {
			log.Println("Error starting transaction:", err)
			return false, true
		}
		defer func() {
			if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
				log.Println("Error during rollback:", err)
			}
		}()

		// Update the alert in the PostgreSQL database
		_, err = tx.Exec(ctx, "UPDATE alerts SET is_triggered = TRUE WHERE id = $1", alert.ID)
		if err != nil {
			log.Println("Error updating alert in PostgreSQL:", err)
			return false, true
		}

		// Determine the reason for the alert
		condition := ""
		if alert.IsAbove {
			condition = fmt.Sprintf("the price of %s has reached or exceeded <strong>$%.2f</strong>.", alert.Coin, alert.Price)
		} else {
			condition = fmt.Sprintf("the price of %s has fallen to or below <strong>$%.2f</strong>.", alert.Coin, alert.Price)
		}

		// Create the email body using the htmlFormat
		emailBody := fmt.Sprintf(htmlFormat,
			alert.Coin,
			alert.Coin,
			alert.TriggeredAt.Format("2006-01-02 15:04:05"),
			alert.CurrentPrice,
			condition,
		)

		// Send the email
		if err = settings.SendEmail(email, "Alert Notification for "+alert.Coin, emailBody, ctx); err != nil {
			log.Println("Error sending email:", err)
			return false, true
		}

		// Commit the transaction if everything went well
		if err = tx.Commit(ctx); err != nil {
			log.Println("Error committing transaction:", err)
			return false, false
		}

		return true, false
	}
}
