package pipeline

import (
	"encoding/json"
	"fmt"
	"hub/pkg/interface/upbit"
	"log"
	"time"
)

type Pipeline[T any, R any] struct {
	in     chan T
	out    chan R
	worker func(T) R
}

func NewPipeline[T any, R any](buf int, worker func(T) R) *Pipeline[T, R] {
	return &Pipeline[T, R]{
		in:     make(chan T, buf),
		out:    make(chan R, buf),
		worker: worker,
	}
}
func ConnectPipeline[T, R, S any](from *Pipeline[T, R], to *Pipeline[R, S]) {
	go func() {
		defer close(to.In())
		for v := range from.Out() {
			to.In() <- v
		}
	}()
}

func (p *Pipeline[T, R]) In() chan<- T { return p.in }
func (p *Pipeline[T, R]) Out() <-chan R { return p.out }

func (p *Pipeline[T, R]) Run() {
	go func() {
		defer close(p.out)
		for v := range p.in {
			res := p.worker(v)
			if _, ok := any(res).(struct{}); ok {
				continue
			}
			p.out <- res
		}
	}()
}

func (p *Pipeline[T, R]) Stop() {
	close(p.in)	
}

func ConvPipeline(exch PlExchange) *Pipeline[*upbit.UpbitRawData, *PlData] {
	return NewPipeline(1000, func (data *upbit.UpbitRawData) *PlData {
		var plData PlData
		plData.CheckPoints = []PlDataCheckpoint{
			{
				name: "recv",
				ts: data.ReceiveTimestamp,
			},
			{
				name: "main",
				ts: data.Timestamp,
			},
			{
				name: "conv",
				ts: time.Now(),
			},
		}
		plData.Exchange = exch
		switch data.Type {
		case upbit.UPBIT_TICKER:
			var d upbit.UpbitTicker
			json.Unmarshal(*data.Bytes, &d)
			plData.DataType = PL_DT_TICKER
			plData.Payload = PlDataTicker {
				Code: NewPlMktCode(d.Code, exch),
				Timestamp: d.Timestamp,
				CurrentPrice: d.TradePrice,
			}
		case upbit.UPBIT_TRADE:
			var d upbit.UpbitTrade
			json.Unmarshal(*data.Bytes, &d)
			plData.DataType = PL_DT_TRADE
			plData.Payload = PlDataTrade {
				Code: NewPlMktCode(d.Code, exch),
				Timestamp: d.Timestamp,
				TradeTimestamp: d.TradeTimestamp,
				TradePrice: d.TradePrice,
				TradeVolume: d.TradeVolume,
			}
		case upbit.UPBIT_ORDERBOOK:
			var d upbit.UpbitOrderbook
			json.Unmarshal(*data.Bytes, &d)
			plData.DataType = PL_DT_ORDERBOOK
			plData.Payload = PlDataOrderbook {
				Code: NewPlMktCode(d.Code, exch),
				Timestamp: d.Timestamp,
				TotalAskSize: d.TotalAskSize,
				TotalBidSize: d.TotalBidSize,
				OrderbookUnits: d.OrderbookUnits,
			}
		case upbit.UPBIT_CANDLE:
			var d upbit.UpbitCandle
			json.Unmarshal(*data.Bytes, &d)
			plData.DataType = PL_DT_CANDLE
			plData.Payload = PlDataCandle {
				Code: NewPlMktCode(d.Code, exch),
				Timestamp: d.Timestamp,
				CandleDateTimeKST: d.CandleDateTimeKST,
				OpeningPrice: d.OpeningPrice,
				HighPrice: d.HighPrice,
				LowPrice: d.LowPrice,
				TradePrice: d.TradePrice,
				CandleAccTradeVolume: d.CandleAccTradeVolume,
				CandleAccTradePrice: d.CandleAccTradePrice,
			}
		case upbit.UPBIT_ERROR:
			var d upbit.UpbitError
			json.Unmarshal(*data.Bytes, &d)
			plData.DataType = PL_DT_ERROR
			plData.Payload = struct{}{}
		}
		return &plData
	})	
}

func LogPipeline() *Pipeline[*PlData, struct{}] {
	return NewPipeline(1000, func (data *PlData) struct {} {
		data.CheckPoints = append(data.CheckPoints, PlDataCheckpoint{name: "log", ts: time.Now()})
		var exch string
		var dt string
		checkpoints := ""
		for i, cp := range data.CheckPoints {
			checkpoints += fmt.Sprintf("%s >> ", cp.name)
			if i != len(data.CheckPoints) -1 {
				checkpoints += fmt.Sprintf("(+%f) >> ", data.CheckPoints[i+1].ts.Sub(cp.ts).Seconds())
			}
		}
		checkpoints += fmt.Sprintf("[ACC +%f]", data.CheckPoints[len(data.CheckPoints)-1].ts.Sub(data.CheckPoints[0].ts).Seconds())
		if data.Exchange == PL_EXCH_UPBIT {
			exch = "UPBIT"
		} else if data.Exchange == PL_EXCH_BITHUMB {
			exch = "BITHUMB"
		}
		if data.DataType == PL_DT_TICKER {
			dt = "TICKER"
			payload := data.Payload.(PlDataTicker)
			log.Printf("%s %s %s %v", exch, dt, payload.Code, checkpoints)
		} else if data.DataType == PL_DT_TRADE {
			dt = "TRADE"
			payload := data.Payload.(PlDataTrade)
			log.Printf("%s %s %s %v", exch, dt, payload.Code, checkpoints)
		} else if data.DataType == PL_DT_ORDERBOOK {
			dt = "ORDERBOOK"
			payload := data.Payload.(PlDataOrderbook)
			log.Printf("%s %s %s %v", exch, dt, payload.Code, checkpoints)
		} else if data.DataType == PL_DT_CANDLE {
			dt = "CANDLE"
			payload := data.Payload.(PlDataCandle)
			log.Printf("%s %s %s %v", exch, dt, payload.Code, checkpoints)
		}
		return struct{}{}
	})
}

