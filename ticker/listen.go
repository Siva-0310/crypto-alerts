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
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	log.Println("WebSocket listener started")

	go func() {
		defer c.Close()
		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled, closing WebSocket connection")
				return
			default:
				_, msg, err := c.ReadMessage()
				if err != nil {
					log.Printf("Error reading message: %v", err)
					time.Sleep(time.Second)
					continue
				}

				if len(msg) == 0 {
					continue
				}

				var scrips map[string]string
				if err := json.Unmarshal(msg, &scrips); err != nil {
					log.Printf("Error unmarshalling JSON: %v", err)
					continue
				}

				for coin, strprice := range scrips {
					price, err := strconv.ParseFloat(strprice, 64)
					if err != nil {
						log.Printf("Error converting price for coin %s: %v", coin, err)
						continue
					}
					coins.Store(coin, price)
				}

			}
		}
	}()

	return c, nil
}
