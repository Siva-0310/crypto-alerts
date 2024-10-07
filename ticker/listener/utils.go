package listener

import (
	"log"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

func CreateWebSocketConn(maxRetries int, url string, header http.Header) (*websocket.Conn, *http.Response, error) {
	var (
		conn *websocket.Conn
		res  *http.Response
		err  error
	)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		conn, res, err = websocket.DefaultDialer.Dial(url, header)
		if err == nil {
			return conn, res, nil
		}
		log.Printf("Failed to connect to WebSocket URL, attempt %d: %v\n", attempt, err)
		time.Sleep((1 << attempt) * time.Second)
	}
	return nil, res, err
}

func isTimeoutError(err error) bool {
	if ne, ok := err.(net.Error); ok {
		return ne.Timeout()
	}
	return false
}
