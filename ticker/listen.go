package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func Listen(url string, coins *sync.Map, ctx context.Context) (*websocket.Conn, error) {
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}
	go func() {
		defer c.Close()
		for {
			select {
			case <-ctx.Done():
				log.Println("Context canceled, closing connection")
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
				}

				for coin, price := range scrips {
					coins.Store(coin, price)
				}

				// You can process the 'scrips' map here
				log.Printf("Received message: %v", scrips)
			}
		}

	}()
	return c, nil
}
