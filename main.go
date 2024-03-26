package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
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
	flag.StringVar(&pair, "pair", "BTCUSDT", "pair to collect data on in uppercase")
	flag.Parse()

	url := fmt.Sprintf("https://api.binance.us/api/v3/depth?symbol=%s", pair)
	duration := time.Hour

	bestBids := make([][10]order, int(duration.Seconds()))
	bestAsks := make([][10]order, int(duration.Seconds()))

	log.Printf("Starting data collection for pair %s", pair)

	startTime := time.Now()
	ticker := time.Tick(time.Second)
	idx := 0

	for now := range ticker {
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error getting order book depth: %v", err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading response body: %v", err)
			continue
		}
		resp.Body.Close()

		var msg message
		if err := json.Unmarshal(body, &msg); err != nil {
			log.Printf("Failed to unmarshal json: %v", err)
			continue
		}

		for i := 0; i < 10; i++ {
			bid := msg.Bids[i]
			if bidPrice, err := strconv.ParseFloat(bid[0], 32); err != nil {
				log.Printf("Failed to parse bid price %v", err)
			} else {
				bestBids[idx][i].price = float32(bidPrice)
			}
			if bidVolume, err := strconv.ParseFloat(bid[1], 32); err != nil {
				log.Printf("Failed to parse bid volume %v", err)
			} else {
				bestBids[idx][i].volume = float32(bidVolume)
			}
		}

		for i := 0; i < 10; i++ {
			ask := msg.Asks[i]
			if askPrice, err := strconv.ParseFloat(ask[0], 32); err != nil {
				log.Printf("Failed to parse ask price %v", err)
			} else {
				bestAsks[idx][i].price = float32(askPrice)
			}
			if askVolume, err := strconv.ParseFloat(ask[1], 32); err != nil {
				log.Printf("Failed to parse ask volume %v", err)
			} else {
				bestAsks[idx][i].volume = float32(askVolume)
			}
		}

		if now.Sub(startTime) >= duration {
			break
		}
		idx++
	}

	var columns [40]string
	for i := 0; i < 10; i++ {
		columns[2*i] = fmt.Sprintf("BidPrice%d", i+1)
		columns[2*i+1] = fmt.Sprintf("BidVolume%d", i+1)
		columns[20+2*i] = fmt.Sprintf("AskPrice%d", i+1)
		columns[20+2*i+1] = fmt.Sprintf("AskVolume%d", i+1)
	}

	file, err := os.Create(fmt.Sprintf("data/%s_%s.csv", pair, time.Now().Format("2006-01-02_15")))
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}

	writer := csv.NewWriter(file)
	writer.Write(columns[:])
	rows := make([][]string, int(duration.Seconds()))
	for i := 0; i < int(duration.Seconds()); i++ {
		row := make([]string, 40)
		for j := 0; j < 10; j++ {
			row[2*j] = fmt.Sprintf("%.5f", bestBids[i][j].price)
			row[2*j+1] = fmt.Sprintf("%.5f", bestBids[i][j].volume)
			row[20+2*j] = fmt.Sprintf("%.5f", bestAsks[i][j].price)
			row[20+2*j+1] = fmt.Sprintf("%.5f", bestAsks[i][j].volume)
		}
		rows[i] = row
	}
	writer.WriteAll(rows)
	writer.Flush()
	log.Printf("Wrote data to file")
}
