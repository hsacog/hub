package pipeline

import (
	"encoding/json"
	"fmt"
	"hub/pkg/interface/upbit"
	"log"
	"math"
	"sort"
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
	return NewPipeline(0, func (data *upbit.UpbitRawData) *PlData {
		var plData PlData
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
		case upbit.UPBIT_CANDLE:
			var d upbit.UpbitCandle
			json.Unmarshal(*data.Bytes, &d)
		case upbit.UPBIT_ERROR:
			var d upbit.UpbitError
			json.Unmarshal(*data.Bytes, &d)
			plData.DataType = PL_Dt_ERROR
			plData.Payload = struct{}{}
		}
		return &plData
	})	
}

func LogPipeline() *Pipeline[*PlData, struct{}] {
	return NewPipeline(0, func (data *PlData) struct {} {
		var exch string
		var dt string
		if data.Exchange == PL_EXCH_UPBIT {
			exch = "UPBIT"
		} else if data.Exchange == PL_EXCH_BITHUMB {
			exch = "BITHUMB"
		}
		if data.DataType == PL_DT_TICKER {
			payload := data.Payload.(PlDataTicker)
			log.Printf("%s %s %s %f", exch, dt, payload.Code, payload.CurrentPrice)
		}
		return struct{}{}
	})
}

func StatePipeline(exch ...PlExchange) *Pipeline[*PlData, PlState] {
	state := make(PlState)
	for _, v := range exch {
		state[v] = make(map[PlMktCode]float64)
	}
	return NewPipeline(0, func (data *PlData) PlState {
		if data.DataType == PL_DT_TICKER {
			code := data.Payload.(PlDataTicker).Code
			price := data.Payload.(PlDataTicker).CurrentPrice
			state[data.Exchange][code] = price
		}
		return state.Copy()
	})
}

func CalcPipeline(ex1 PlExchange, ex2 PlExchange) *Pipeline[PlState, struct{}] {
	return NewPipeline(0, func (state PlState) struct{} {
		// compare ex1 and ex2 using both existing mktcodes		
		type Cmp struct {
			code PlMktCode
			ex1Value float64
			ex2Value float64
			diff float64
			diffRatio float64
		}
		cmps := []Cmp{}
		for k, v1 := range (state)[ex1] {
			if k.C1 != "KRW" {
				continue
			}
			if v2, ok := (state)[ex2][k]; ok {
				diff := math.Abs(v1 - v2)
				diffRatio := diff / ((v1 + v2)/2)
				cmps = append(cmps, Cmp{k, v1, v2, diff, diffRatio})
			}
		}
		sort.Slice(cmps, func(i, j int) bool {
			return cmps[i].diffRatio > cmps[j].diffRatio
		})
		if len(cmps) > 0 {
			msg := fmt.Sprintf("[%s] ", cmps[0].code.C1 + "-" + cmps[0].code.C2)
			msg += fmt.Sprintf("%s:%f ", PlExchangeMap[ex1], cmps[0].ex1Value)
			msg += fmt.Sprintf("%s:%f ", PlExchangeMap[ex2], cmps[0].ex2Value)
			msg += fmt.Sprintf("<%f>", math.Abs(cmps[0].ex1Value - cmps[0].ex2Value))
			log.Println(msg)
		}

		return struct{}{}
	})
}
