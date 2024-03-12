package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"nhooyr.io/websocket"
)

type order struct {
	price  float32
	volume float32
}

type message struct {
	LastUpdateID int         `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
}

func main() {
	var pair string
	flag.StringVar(&pair, "pair", "btcusdt", "pair to collect data on")
	flag.Parse()

	uri := fmt.Sprintf("wss://stream.binance.us:9443/ws/%s@depth10", pair)
	var columns [40]string
	for i := 0; i < 10; i++ {
		columns[2*i] = fmt.Sprintf("BidPrice%d", i+1)
		columns[2*i+1] = fmt.Sprintf("BidVolume%d", i+1)
		columns[20+2*i] = fmt.Sprintf("AskPrice%d", i+1)
		columns[20+2*i+1] = fmt.Sprintf("AskVolume%d", i+1)
	}

	bestBids := make([][10]order, int(time.Hour.Seconds()))
	bestAsks := make([][10]order, int(time.Hour.Seconds()))

	startTime := time.Now()

	ctx := context.Background()
	conn, _, err := websocket.Dial(ctx, uri, nil)
	if err != nil {
		log.Fatal("Failed to connect to websocket server")
	}

	log.Printf("Starting data collection for pair %s", pair)
	elapsedSeconds := 0
	for time.Since(startTime) < time.Hour {
		var msg message
		_, raw, err := conn.Read(ctx)
		if err != nil {
			log.Println("Error reading message from websocket")
			break
		}

		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Println("Failed to unmarshal json")
			break
		}

		for i, bid := range msg.Bids {
			if bidPrice, err := strconv.ParseFloat(bid[0], 32); err != nil {
				log.Fatal("Failed to parse bid price")
			} else {
				bestBids[elapsedSeconds][i].price = float32(bidPrice)
			}
			if bidVolume, err := strconv.ParseFloat(bid[1], 32); err != nil {
				log.Fatal("Failed to parse bid volume")
			} else {
				bestBids[elapsedSeconds][i].volume = float32(bidVolume)
			}
		}

		for i, ask := range msg.Asks {
			if askPrice, err := strconv.ParseFloat(ask[0], 32); err != nil {
				log.Fatal("Failed to parse ask price")
			} else {
				bestAsks[elapsedSeconds][i].price = float32(askPrice)
			}
			if askVolume, err := strconv.ParseFloat(ask[1], 32); err != nil {
				log.Fatal("Failed to parse ask volume")
			} else {
				bestAsks[elapsedSeconds][i].volume = float32(askVolume)
			}
		}

		if elapsedSeconds%60 == 0 {
			log.Printf("Collected data for second %d", elapsedSeconds)
		}
		elapsedSeconds++
	}

	file, err := os.Create(fmt.Sprintf("data/%s_%s.csv", pair, time.Now().Format("2006-01-02_15")))
	if err != nil {
		log.Fatal("Failed to create output file")
	}
	writer := csv.NewWriter(file)

	writer.Write(columns[:])
	rows := make([][]string, int(time.Hour.Seconds()))
	for i := 0; i < int(time.Hour.Seconds()); i++ {
		row := make([]string, 40)
		for j := 0; j < 10; j++ {
			row[2*j] = fmt.Sprintf("%.2f", bestBids[i][j].price)
			row[2*j+1] = fmt.Sprintf("%.2f", bestBids[i][j].volume)
			row[20+2*j] = fmt.Sprintf("%.2f", bestAsks[i][j].price)
			row[20+2*j+1] = fmt.Sprintf("%.2f", bestAsks[i][j].volume)
		}
		rows[i] = row
	}
	writer.WriteAll(rows)
	writer.Flush()
	log.Printf("Wrote data to file")
}
