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
		},
	)
	/* if err := uif.Open(); err != nil {
		log.Fatal(err)
	} */
	uif.Run()

	bif, bch := upbit.NewUpbitIF(
		upbit.UpbitIFConfig{
			AccessKey: os.Getenv("BITHUMB_ACCESS_KEY"),
			SecretKey: os.Getenv("BITHUMB_SECRET_KEY"),
			QuoUrl: url.URL{Scheme: "wss", Host: "ws-api.bithumb.com", Path: "websocket/v1"},
			ExcUrl: url.URL{Scheme: "wss", Host: "ws-api.bithumb.com", Path: "websocket/v1/private"},
		},
	)
	bif.Run()

	// pipelines
	logPipe := pipeline.LogPipeline()
	logPipe.Run()
	defer logPipe.Stop()

	/* calcPipe := pipeline.CalcPipeline(pipeline.PL_EXCH_UPBIT, pipeline.PL_EXCH_BITHUMB)
	calcPipe.Run()
	defer calcPipe.Stop()

	statePipe := pipeline.StatePipeline(pipeline.PL_EXCH_UPBIT, pipeline.PL_EXCH_BITHUMB)
	statePipe.Run()
	defer statePipe.Stop()
	pipeline.ConnectPipeline(statePipe, calcPipe) */

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
			if com.Type == command.ADD_MKT {
				log.Println("subscribe", com.Payload)
				var mktPairs []command.MktPair
				for _, p := range com.Payload {
					mktPairs = append(mktPairs, p.(command.MktPair))
				}
				uif.Subscribe(mktPairs)
				bif.Subscribe(mktPairs)
			} else if com.Type == command.REMOVE_MKT {
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
