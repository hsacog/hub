package task

import (
	"encoding/json"
	"fmt"
	"hub/pkg/gateway/upbit"
	"log"
	"slices"
	"sync"
	"time"
)

type Task[T any, R any] struct {
	in     chan T
	out    chan R
	once sync.Once
	worker func(T) R
}

type Tasker interface {
	Run()
	Stop()
}

func NewTask[T any, R any](buf int, worker func(T) R) *Task[T, R] {
	return &Task[T, R]{
		in:     make(chan T, buf),
		out:    make(chan R, buf),
		worker: worker,
	}
}

func (p *Task[T, R]) In() chan<- T { return p.in }
func (p *Task[T, R]) Out() <-chan R { return p.out }

func (p *Task[T, R]) Run() {
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

func (p *Task[T, R]) Stop() {
	p.once.Do(func() {
		close(p.in)
	})
}

func ConvTask(exch PlExchange) *Task[*upbit.UpbitRawData, *PlData] {
	return NewTask(1000, func (data *upbit.UpbitRawData) *PlData {
		var plData PlData
		plData.CheckPoints = []PlDataCheckpoint{
			{
				Name: "recv",
				Ts: data.ReceiveTimestamp,
				Err: nil,
			},
			{
				Name: "main",
				Ts: data.Timestamp,
				Err: nil,
			},
		}
		plData.AddCp("conv", nil)
		plData.Exchange = exch
		switch data.Type {
		case upbit.UPBIT_TICKER:
			var d upbit.UpbitTicker
			json.Unmarshal(*data.Bytes, &d)
			plData.DataType = PL_DT_TICKER
			plData.Payload = PlDataTicker {
				Code: NewPlMktCode(d.Code, exch),
				TradePrice: d.TradePrice,
				SignedChangePrice: d.SignedChangePrice,
				SignedChangeRate: d.SignedChangeRate,
				AccTradePrice: d.AccTradePrice,
				AccTradePrice24h: d.AccTradePrice24h,
				Timestamp: d.Timestamp,
			}
			plData.Timestamp = d.Timestamp // use received data timestamp
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
			plData.Timestamp = d.Timestamp // use received data timestamp
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
			plData.Timestamp = d.Timestamp // use received data timestamp
		case upbit.UPBIT_CANDLE:
			var d upbit.UpbitCandle
			json.Unmarshal(*data.Bytes, &d)
			plData.DataType = PL_DT_CANDLE
			plData.Payload = PlDataCandle {
				Code: NewPlMktCode(d.Code, exch),
				CandleDateTimeUTC: d.CandleDateTimeUTC,
				CandleDateTimeKST: d.CandleDateTimeKST,
				OpeningPrice: d.OpeningPrice,
				HighPrice: d.HighPrice,
				LowPrice: d.LowPrice,
				TradePrice: d.TradePrice,
				CandleAccTradeVolume: d.CandleAccTradeVolume,
				CandleAccTradePrice: d.CandleAccTradePrice,
				Timestamp: d.Timestamp,
			}
			plData.Timestamp = d.Timestamp // use received data timestamp
		case upbit.UPBIT_ERROR:
			var d upbit.UpbitError
			json.Unmarshal(*data.Bytes, &d)
			log.Println("UPBIT_ERROR: ", d)
			plData.DataType = PL_DT_ERROR
			plData.Payload = struct{}{}
			plData.Timestamp = time.Now().UnixMilli() // use system timestamp
		}
		return &plData
	})	
}

type LogMode int8
const (
	LOG_METRIC LogMode = iota
	LOG_VIEW
	LOG_NONE
) 

func LogTask(mode LogMode, dts ...PlDataType) *Task[*PlData, *PlData] {
	return NewTask(1000, func(data *PlData) *PlData {
		data.AddCp("log", nil)
		exch := PlExchangeMap[data.Exchange]
		dt := PlDataTypeMap[data.DataType]
		var mktCode PlMktCode
		var value string
		if data.DataType == PL_DT_TICKER {
			payload := data.Payload.(PlDataTicker)
			mktCode = payload.Code
			value = fmt.Sprintf("<%s> %f", time.UnixMilli(payload.Timestamp).String(), payload.TradePrice)
		} else if data.DataType == PL_DT_TRADE {
			payload := data.Payload.(PlDataTrade)
			mktCode = payload.Code
			value = fmt.Sprintf("<%s> %f", time.UnixMilli(payload.Timestamp).String(), payload.TradePrice)
		} else if data.DataType == PL_DT_ORDERBOOK {
			payload := data.Payload.(PlDataOrderbook)
			mktCode = payload.Code
			value = fmt.Sprintf("%v", payload)
		} else if data.DataType == PL_DT_CANDLE {
			payload := data.Payload.(PlDataCandle)
			mktCode = payload.Code
			value = fmt.Sprintf("<%s> %f %f %f %f", time.UnixMilli(payload.Timestamp).String(), payload.OpeningPrice, payload.HighPrice, payload.LowPrice, payload.TradePrice)
		}

		if slices.Contains(dts, data.DataType) {
			if mode == LOG_METRIC {
				log.Printf("%s %s %s %v", exch, dt, mktCode, data.LogCp())
			} else if mode == LOG_VIEW {
				log.Printf("%s %s %s", exch, dt, value)
			}
		}
		return data
	})
}

func NullTask[T any]() *Task[T, struct{}] {
	return NewTask(0, func(data T) struct{} {
		return struct{}{}
	})	
}

