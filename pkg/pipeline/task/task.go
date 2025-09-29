package task

import (
	"encoding/json"
	"fmt"
	"hub/pkg/gateway/upbit"
	"log"
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
func TaskConnector[T, R, S any](from *Task[T, R], to *Task[R, S]) {
	go func() {
		defer to.Stop()
		for v := range from.Out() {
			to.In() <- v
		}
	}()
}
func TaskMetricConnector[T, R, S any](from *Task[T, R], to *Task[R, S], dur time.Duration) {
	ticker := time.NewTicker(dur)	
	var cnt int64 = 0
	go func() {
		defer to.Stop()
		for {
			select {
			case v, ok := <-from.Out():
				if !ok {
					return
				} else {
					to.In() <- v
					cnt += 1
				}
			case t := <-ticker.C:
				log.Printf("[METRIC] DUR(%s) TIME(%s) THROUGHPUT: %d\n", dur.String(), t.String(), cnt)
				cnt = 0	
			}
		}
	}()
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
			{
				Name: "conv",
				Ts: time.Now(),
				Err: nil,
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
				TradePrice: d.TradePrice,
				SignedChangePrice: d.SignedChangePrice,
				SignedChangeRate: d.SignedChangeRate,
				AccTradePrice: d.AccTradePrice,
				AccTradePrice24h: d.AccTradePrice24h,
				Timestamp: d.Timestamp,
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
		case upbit.UPBIT_ERROR:
			var d upbit.UpbitError
			json.Unmarshal(*data.Bytes, &d)
			log.Println("UPBIT_ERROR: ", d)
			plData.DataType = PL_DT_ERROR
			plData.Payload = struct{}{}
		}
		return &plData
	})	
}

func LogTask(mode bool) *Task[*PlData, *PlData] {
	return NewTask(1000, func(data *PlData) *PlData {
		data.CheckPoints = append(data.CheckPoints, PlDataCheckpoint{Name: "log", Ts: time.Now(), Err: nil})
		var exch string
		var dt string
		checkpoints := ""
		for i, cp := range data.CheckPoints {
			checkpoints += fmt.Sprintf("%s >> ", cp.Name)
			if i != len(data.CheckPoints) -1 {
				checkpoints += fmt.Sprintf("(+%f) >> ", data.CheckPoints[i+1].Ts.Sub(cp.Ts).Seconds())
			}
		}
		checkpoints += fmt.Sprintf("[ACC +%f]", data.CheckPoints[len(data.CheckPoints)-1].Ts.Sub(data.CheckPoints[0].Ts).Seconds())
		if data.Exchange == PL_EXCH_UPBIT {
			exch = "UPBIT"
		} else if data.Exchange == PL_EXCH_BITHUMB {
			exch = "BITHUMB"
		}
		if data.DataType == PL_DT_TICKER {
			dt = "TICKER"
			payload := data.Payload.(PlDataTicker)
			if mode {
				log.Printf("%s %s %s %v", exch, dt, payload.Code, checkpoints)
			}
		} else if data.DataType == PL_DT_TRADE {
			dt = "TRADE"
			payload := data.Payload.(PlDataTrade)
			if mode {
				log.Printf("%s %s %s %v", exch, dt, payload.Code, checkpoints)
			}
		} else if data.DataType == PL_DT_ORDERBOOK {
			dt = "ORDERBOOK"
			payload := data.Payload.(PlDataOrderbook)
			if mode {
				log.Printf("%s %s %s %v", exch, dt, payload.Code, checkpoints)
			}
		} else if data.DataType == PL_DT_CANDLE {
			dt = "CANDLE"
			payload := data.Payload.(PlDataCandle)
			if mode {
				log.Printf("%s %s %s %v", exch, dt, payload.Code, checkpoints)
			}
		}
		return data
	})
}

func NullTask() *Task[*PlData, struct{}] {
	return NewTask(0, func(data *PlData) struct{} {
		return struct{}{}
	})	
}

