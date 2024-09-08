package main

import (
	"context"
	"log"
	"sync"
	"time"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coins := &sync.Map{}

	_, err := Listen("wss://ws.coincap.io/prices?assets=bitcoin,ethereum,monero,litecoin", coins, ctx)
	if err != nil {
		log.Println(err)
	}
	compressor := Compresser{
		Duration: time.Duration(5 * time.Second),
	}
	compressor.Start(coins, ctx)
	for {

	}

}
