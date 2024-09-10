package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

func isTimeoutError(err error) bool {
	if ne, ok := err.(net.Error); ok {
		return ne.Timeout()
	}
	return false
}

func CreateWebSocketConn(url string, header http.Header) (*websocket.Conn, *http.Response, error) {
	var (
		c   *websocket.Conn
		err error
		res *http.Response
	)
	for i := 0; i < 5; i++ {
		c, res, err = websocket.DefaultDialer.Dial(url, nil)
		if err == nil {
			return c, res, nil
		}
		log.Printf("Failed to connect to WebSocket Url, attempt %d: %v\n", i+1, err)
		time.Sleep((2 << i) * time.Second)
	}
	return c, res, err
}

// Listen connects to a WebSocket server and listens for incoming coin price updates.
func Listen(url string, coins *sync.Map, wg *sync.WaitGroup, ctx context.Context, errsig chan error) (*websocket.Conn, error) {
	c, _, err := CreateWebSocketConn(url, nil)
	if err != nil {
		return nil, err
	}

	log.Println("WebSocket listener started")
	wg.Add(1)

	go func() {
		defer func() {
			if c != nil {
				c.Close()
			}
			wg.Done()
		}()

		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled, closing WebSocket connection")
				return
			default:
				err := c.SetReadDeadline(time.Now().Add(10 * time.Second))
				if err != nil {
					log.Printf("Error setting read deadline: %v", err)
					errsig <- err
					return
				}
				_, msg, err := c.ReadMessage()
				if err != nil {
					log.Printf("Error reading message: %v", err)
					if websocket.IsCloseError(err, websocket.CloseAbnormalClosure, websocket.CloseGoingAway) || isTimeoutError(err) {
						if c != nil {
							c.Close()
						}
						c, _, err = CreateWebSocketConn(url, nil)
						if err != nil {
							log.Printf("Failed to reconnect to WebSocket URL: %v", err)
							errsig <- err
							return
						}
						log.Println("Reconnected to WebSocket URL successfully")
					} else if websocket.IsUnexpectedCloseError(err, websocket.CloseAbnormalClosure, websocket.CloseGoingAway) {
						log.Println("Unexpected WebSocket closure.")
						errsig <- err
						return
					}
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
