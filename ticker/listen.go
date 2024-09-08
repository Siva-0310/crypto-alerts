package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Listen connects to a WebSocket server and listens for incoming coin price updates.
func Listen(url string, coins *sync.Map, ctx context.Context) (*websocket.Conn, error) {
	// Connect to the WebSocket server
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	// Start a goroutine to listen for messages and update the coins map
	go func() {
		defer c.Close()
		for {
			select {
			case <-ctx.Done():
				// When the context is canceled, close the connection
				log.Println("Context canceled, closing connection")
				return
			default:
				// Read a message from the WebSocket connection
				_, msg, err := c.ReadMessage()
				if err != nil {
					log.Printf("Error reading message: %v", err)
					time.Sleep(time.Second)
					continue
				}

				// Skip empty messages
				if len(msg) == 0 {
					continue
				}

				// Parse the message into a map of coin names and string prices
				var scrips map[string]string
				if err := json.Unmarshal(msg, &scrips); err != nil {
					log.Printf("Error unmarshalling JSON: %v", err)
					continue
				}

				// Convert the string prices to float64 and store them in the coins map
				for coin, strprice := range scrips {
					price, err := strconv.ParseFloat(strprice, 64)
					if err != nil {
						log.Printf("Error converting price for coin %s: %v", coin, err)
						continue
					}
					coins.Store(coin, price) // Store the coin and its price in the sync.Map
				}

			}
		}
	}()

	// Return the WebSocket connection
	return c, nil
}
