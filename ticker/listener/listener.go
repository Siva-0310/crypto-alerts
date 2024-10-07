package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Listener struct {
	WebSocketURL string
	Db           *sync.Map
	c            *websocket.Conn
}

func NewListener(url string, db *sync.Map) *Listener {
	return &Listener{
		WebSocketURL: url,
		Db:           db,
	}
}

func (l *Listener) Init(maxRetries int) error {
	c, _, err := CreateWebSocketConn(maxRetries, l.WebSocketURL, nil)
	if err != nil {
		return fmt.Errorf("failed to initialize WebSocket connection after %d attempts: %v", maxRetries, err)
	}
	l.c = c
	return nil
}

func (l *Listener) Listen(maxRetries int, duration time.Duration, wg *sync.WaitGroup, errsig chan error, ctx context.Context) {
	wg.Add(1)
	go func() {
		defer func() {
			l.Close()
			wg.Done()
		}()
		for {
			select {
			case <-ctx.Done():
				log.Println("Context cancelled, closing WebSocket connection")
				return
			default:
				// Set the read deadline
				err := l.c.SetReadDeadline(time.Now().Add(duration))
				if err != nil {
					errsig <- fmt.Errorf("failed to set read deadline: %v", err)
					return
				}

				// Read message from WebSocket
				_, msg, err := l.c.ReadMessage()
				if err != nil {
					if websocket.IsCloseError(err, websocket.CloseAbnormalClosure, websocket.CloseGoingAway) || isTimeoutError(err) {
						log.Printf("WebSocket connection issue: %v", err)
						if l.c != nil {
							l.c.Close()
						}
						// Attempt to reconnect
						l.c, _, err = CreateWebSocketConn(maxRetries, l.WebSocketURL, nil)
						if err != nil {
							errsig <- fmt.Errorf("failed to reconnect to WebSocket URL: %v", err)
							return
						}
						log.Println("Reconnected to WebSocket URL successfully")
					} else if websocket.IsUnexpectedCloseError(err, websocket.CloseAbnormalClosure, websocket.CloseGoingAway) {
						errsig <- fmt.Errorf("unexpected WebSocket closure: %v", err)
						return
					}
					log.Printf("Error reading message: %v", err)
					time.Sleep(time.Second)
					continue
				}

				// Skip empty messages
				if len(msg) == 0 {
					continue
				}

				// Unmarshal JSON message
				var scrips map[string]string
				if err := json.Unmarshal(msg, &scrips); err != nil {
					log.Printf("Error unmarshalling JSON: %v", err)
					continue
				}

				// Process each coin and store its price in the database
				for coin, strprice := range scrips {
					price, err := strconv.ParseFloat(strprice, 64)
					if err != nil {
						log.Printf("Error converting price for coin %s: %v", coin, err)
						continue
					}
					l.Db.Store(coin, price)
				}
			}
		}
	}()
}

func (l *Listener) Close() {
	if l.c != nil {
		l.c.Close()
	}
}
