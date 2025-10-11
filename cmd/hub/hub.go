package main

import (
	"hub/pkg/command"
	"hub/pkg/command/api"
	"hub/pkg/gateway"
	"hub/pkg/pipeline"
	"io"

	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	// configure logger
	file, err := os.OpenFile("app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	mw := io.MultiWriter(os.Stdout, file)
	log.SetOutput(mw)

	log.Println("STARTING IF")
	log.SetFlags(log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	// configure command receiver
	cmd_ch := make(chan command.Command)
	cr := api.NewRestApiRunner(cmd_ch)
	cr.Start()

	// configure interfaces
	uif, uch := gateway.NewUpbitGateway()
	uif.Run()

	bif, bch := gateway.NewBithumbGateway()
	bif.Run()
	
	var pl *pipeline.Pipeline
	switch (os.Getenv("MODE")) {
	case "PROD":
		pl, err = pipeline.CryptoProdPipeline()
	case "DIFF":
		pl, err = pipeline.DiffPipeline()
	default:
		pl, err = pipeline.CryptoPipeline()
		
	}
	if err != nil {
		log.Fatal(err)
	}
	pl.Run()

	for {
		select {
		case com := <-cmd_ch:
			switch com.Type {
			case command.ADD_MKT:
				// log.Println("subscribe", com.Payload)
				var mktPairs []command.MktPair
				for _, p := range com.Payload {
					mktPairs = append(mktPairs, p.(command.MktPair))
				}
				uif.Subscribe(mktPairs)
				bif.Subscribe(mktPairs)
			case command.REMOVE_MKT:
				// log.Println("unsubscribe", com.Payload)
				var mktPairs []command.MktPair
				for _, p := range com.Payload {
					mktPairs = append(mktPairs, p.(command.MktPair))
				}
				uif.UnSubscribe(mktPairs)
				bif.UnSubscribe(mktPairs)
			case command.RESET:
				log.Println("[cmd] RESET")
				uif.Reset()
				bif.Reset()
			}
		case data := <-uch:
			if err := pipeline.PlInput(pl, "in1", &data); err != nil {
				log.Println(err)
			}
		case data := <-bch:
			if err := pipeline.PlInput(pl, "in2", &data); err != nil {
				log.Println(err)
			}
		}
	}
}
