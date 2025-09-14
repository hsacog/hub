package calc

import (
	"fmt"
	. "hub/pkg/pipeline"
	"log"
	"math"
	"sort"
)

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
