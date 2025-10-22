package calc

import (
	"fmt"
	. "hub/pkg/pipeline/task"
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

type PlMemoryBox[T any] struct {
	t time.Time
	value T
};
type PlMemory[T any] struct {
	lk sync.RWMutex
	name string
	data []PlMemoryBox[T]
}

func MemoryTask[T PlDataPayload](name string, dur time.Duration) *Task[*PlData, *PlMemory[T]] {
	mem := make(map[string]*PlMemory[T])
	/* mem := PlMemory[T]{
		data: make([]PlMemoryBox[T], 0),
	} */
	return NewTask(0, func (data *PlData) *PlMemory[T] {
		key := data.Payload.(T).Key()
		_, ok := mem[key]
		if !ok {
			mem[key] = &PlMemory[T]{
				name: name,
				data: []PlMemoryBox[T]{},
			}
		}
		mem[key].lk.Lock()
		t := time.UnixMilli(data.Timestamp)
		
		mem[key].data = append(mem[key].data, PlMemoryBox[T]{
			t: t,
			value: data.Payload.(T),
		})
		lim := t.Add(-1 * dur)
		
		idx := 0
		for i, m := range mem[key].data {
			if m.t.After(lim) {
				idx = max(0, i-1)
				break
			}
		}
		mem[key].data = mem[key].data[idx:]
		// log.Printf("mem len: %d", len(mem.data))
		mem[key].lk.Unlock()
		return mem[key]
	})
}

type PlRate struct {
	Mem string
	Key string
	T1 time.Time
	T2 time.Time
	Sp float64
	Hp float64
	Lp float64
	Ep float64
	R float64
}
func FlucRateTask[T any]() *Task[*PlMemory[T], *PlRate] {
	return NewTask(0, func (mem *PlMemory[T]) *PlRate {
		mem.lk.RLock()
		var t1, t2 time.Time
		var mkt1, mkt2 string
		var sp, hp, lp, ep, r float64

		var v1 any = mem.data[0].value
		var v2 any = mem.data[len(mem.data)-1].value
		switch v1.(type) {
		case PlDataTicker:
			t1 = mem.data[0].t
			mkt1 = v1.(PlDataTicker).Key()
			// v1p := v1.(PlDataTicker).TradePrice

			t2 = mem.data[len(mem.data)-1].t
			mkt2 = v2.(PlDataTicker).Key()
			// v2p := v2.(PlDataTicker).TradePrice
			
			if (mkt1 != mkt2) {
				log.Fatal("invalid state")
			}

			// r = ((v2p-v1p)/v1p)*100
			// log.Printf("%s :: [%s %s] %f >> [%s %s] %f :: %f%%", mem.name, mkt1, t1.Format("15:04:05"), v1p, mkt2, t2.Format("15:04:05"), v2p, r)

			//accumulate all fluctuation
			sp = any(mem.data[0].value).(PlDataTicker).TradePrice
			hp = sp
			lp = sp
			ep = any(mem.data[len(mem.data)-1].value).(PlDataTicker).TradePrice
			r = ((ep-sp)/sp)*100
			for _, b := range mem.data {
				p := any(b.value).(PlDataTicker).TradePrice
				hp = math.Max(hp, p)
				lp = math.Min(lp, p)
				/* if i == len(mem.data) - 1 {
					break;
				}	
				var v1 any = mem.data[i].value
				var v2 any = mem.data[i+1].value
				v1p := v1.(PlDataTicker).TradePrice
				v2p := v2.(PlDataTicker).TradePrice
				r += ((v2p - v1p)/v1p)*100 */
			}
		}

		mem.lk.RUnlock()
		return &PlRate{
			Mem: mem.name,	
			Key: mkt1,
			T1: t1,
			T2: t2,
			Sp: sp,
			Hp: hp,
			Lp: lp,
			Ep: ep,
			R: r,
		}
	})
}

type PlState map[PlExchange]map[PlMktCode]float64
func (s PlState) Copy() PlState {
    copyState := make(PlState, len(s))
    for exch, inner := range s {
        innerCopy := make(map[PlMktCode]float64, len(inner))
        for code, val := range inner {
            innerCopy[code] = val
        }
        copyState[exch] = innerCopy
    }
    return copyState
}

func StateTask(exch ...PlExchange) *Task[*PlData, PlState] {
	state := make(PlState)
	for _, v := range exch {
		state[v] = make(map[PlMktCode]float64)
	}
	return NewTask(0, func (data *PlData) PlState {
		if data.DataType == PL_DT_TICKER {
			code := data.Payload.(PlDataTicker).Code
			price := data.Payload.(PlDataTicker).TradePrice
			state[data.Exchange][code] = price
		}
		return state.Copy()
	})
}

func CalcTask(ex1 PlExchange, ex2 PlExchange) *Task[PlState, struct{}] {
	return NewTask(0, func (state PlState) struct{} {
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
