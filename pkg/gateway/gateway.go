package gateway

import (
	"hub/pkg/gateway/upbit"
	"net/url"
	"os"
)

func NewUpbitGateway() (*upbit.UpbitIF, chan upbit.UpbitRawData) {
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
	return uif, uch
}

func NewBithumbGateway() (*upbit.UpbitIF, chan upbit.UpbitRawData) {
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
	return bif, bch
}
