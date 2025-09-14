package main

import (
	"fmt"
	"hub/pkg/command"
	"hub/pkg/command/api"
	"net/url"

	// "hub/pkg/interface/bithumb"
	"hub/pkg/interface/upbit"
	"hub/pkg/pipeline"
	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	fmt.Println("STARTING IF")
	log.SetFlags(log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}
	// configure command receiver
	cmd_ch := make(chan command.Command)
	cr := api.NewRestApiRunner(cmd_ch)
	cr.Start()

	// configure interfaces
	uif, uch := upbit.NewUpbitIF(
		upbit.UpbitIFConfig{
			AccessKey: os.Getenv("UPBIT_ACCESS_KEY"),
			SecretKey: os.Getenv("UPBIT_SECRET_KEY"),
			QuoUrl: url.URL{Scheme: "wss", Host: "api.upbit.com", Path: "websocket/v1"},
			ExcUrl: url.URL{Scheme: "wss", Host: "api.upbit.com", Path: "websocket/v1/private"},
			Options: []upbit.UpbitIFDataOption{
				{
					Type: upbit.UPBIT_TICKER,
					Option: upbit.UpbitDTOption{
						IsOnlyRealtime: true,	
					},
				},
				{
					Type: upbit.UPBIT_TRADE,
					Option: upbit.UpbitDTOption{
						IsOnlyRealtime: true,	
					},
				},
				{
					Type: upbit.UPBIT_ORDERBOOK,
					Option: upbit.UpbitDTOption{
						IsOnlyRealtime: true,
						OrderbookUnitSize: 5,
					},
				},
				{
					Type: upbit.UPBIT_CANDLE,
					Option: upbit.UpbitDTOption{
						IsOnlyRealtime: true,
						CandleInterval: "1s",
					},
				},
			},
		},
	)
	uif.Run()

	bif, bch := upbit.NewUpbitIF(
		upbit.UpbitIFConfig{
			AccessKey: os.Getenv("BITHUMB_ACCESS_KEY"),
			SecretKey: os.Getenv("BITHUMB_SECRET_KEY"),
			QuoUrl: url.URL{Scheme: "wss", Host: "ws-api.bithumb.com", Path: "websocket/v1"},
			ExcUrl: url.URL{Scheme: "wss", Host: "ws-api.bithumb.com", Path: "websocket/v1/private"},
			Options: []upbit.UpbitIFDataOption{
				{
					Type: upbit.UPBIT_TICKER,
					Option: upbit.UpbitDTOption{
						IsOnlyRealtime: true,	
					},
				},
				{
					Type: upbit.UPBIT_TRADE,
					Option: upbit.UpbitDTOption{
						IsOnlyRealtime: true,	
					},
				},
				{
					Type: upbit.UPBIT_ORDERBOOK,
					Option: upbit.UpbitDTOption{
						IsOnlyRealtime: true,
						OrderbookUnitSize: 5,
					},
				},
			},
		},
	)
	bif.Run()

	// pipelines
	logPipe := pipeline.LogPipeline()
	logPipe.Run()
	defer logPipe.Stop()

	upbitConvPipe := pipeline.ConvPipeline(pipeline.PL_EXCH_UPBIT)
	upbitConvPipe.Run()
	defer upbitConvPipe.Stop()
	pipeline.ConnectPipeline(upbitConvPipe, logPipe)

	bithumbConvPipe := pipeline.ConvPipeline(pipeline.PL_EXCH_BITHUMB)
	bithumbConvPipe.Run()
	defer bithumbConvPipe.Stop()
	pipeline.ConnectPipeline(bithumbConvPipe, logPipe)

	for {
		select {
		case com := <-cmd_ch:
			switch com.Type {
			case command.ADD_MKT:
				log.Println("subscribe", com.Payload)
				var mktPairs []command.MktPair
				for _, p := range com.Payload {
					mktPairs = append(mktPairs, p.(command.MktPair))
				}
				uif.Subscribe(mktPairs)
				bif.Subscribe(mktPairs)
			case command.REMOVE_MKT:
				log.Println("unsubscribe", com.Payload)
				var mktPairs []command.MktPair
				for _, p := range com.Payload {
					mktPairs = append(mktPairs, p.(command.MktPair))
				}
				uif.UnSubscribe(mktPairs)
				bif.UnSubscribe(mktPairs)
				
			}
		case data := <-uch:
			upbitConvPipe.In() <- &data
		case data := <-bch:
			bithumbConvPipe.In() <- &data
		}
	}
}
