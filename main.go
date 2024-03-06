package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
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
	uri := "wss://stream.binance.us:9443/ws/btcusdt@depth10"
	var columns [40]string
	for i := 0; i < 10; i++ {
		columns[2*i] = fmt.Sprintf("BidPrice%d", i+1)
		columns[2*i+1] = fmt.Sprintf("BidVolume%d", i+1)
		columns[20+2*i] = fmt.Sprintf("AskPrice%d", i+1)
		columns[20+2*i+1] = fmt.Sprintf("AskVolume%d", i+1)
	}
	numOrders := 0

	bestBids := make([][10]order, 24*60*60)
	bestAsks := make([][10]order, 24*60*60)

	file, err := os.Create(fmt.Sprintf("data_%s.csv", time.Now().Format("2024-03-05")))
	if err != nil {
		log.Fatal("Failed to create output file")
	}
	writer := csv.NewWriter(file)

	ctx := context.Background()
	conn, _, err := websocket.Dial(ctx, uri, nil)
	if err != nil {
		log.Fatal("Failed to connect to websocket server")
	}

	for {
		var msg message
		_, raw, err := conn.Read(ctx)
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			log.Println("Websocket closed connection")
			break
		} else if err != nil {
			log.Fatal("Failed to read message from websocket")
		}
		if err = json.Unmarshal(raw, &msg); err != nil {
			log.Fatal("Failed to unmarshal json")
		}
		for i, bid := range msg.Bids {
			if bidPrice, err := strconv.ParseFloat(bid[0], 32); err != nil {
				log.Fatal("Failed to parse bid price")
			} else {
				bestBids[numOrders][i].price = float32(bidPrice)
			}
			if bidVolume, err := strconv.ParseFloat(bid[1], 32); err != nil {
				log.Fatal("Failed to parse bid volume")
			} else {
				bestBids[numOrders][i].volume = float32(bidVolume)
			}
		}
		for i, ask := range msg.Asks {
			if askPrice, err := strconv.ParseFloat(ask[0], 32); err != nil {
				log.Fatal("Failed to parse ask price")
			} else {
				bestAsks[numOrders][i].price = float32(askPrice)
			}
			if askVolume, err := strconv.ParseFloat(ask[1], 32); err != nil {
				log.Fatal("Failed to parse ask volume")
			} else {
				bestAsks[numOrders][i].volume = float32(askVolume)
			}
		}
		numOrders++
	}

	writer.Write(columns[:])
	rows := make([][]string, numOrders)
	for i := 0; i < numOrders; i++ {
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
}
