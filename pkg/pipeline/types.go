package pipeline

import (
	"hub/pkg/interface/upbit"
	"strings"
	"time"
)

type PlExchange uint8
const (
	PL_EXCH_UPBIT PlExchange = iota
	PL_EXCH_BITHUMB	
)
var PlExchangeMap = map[PlExchange]string{
	PL_EXCH_BITHUMB: "BITHUMB",
	PL_EXCH_UPBIT: "UPBIT",
}

type PlDataType uint8
const (
	PL_DT_TICKER PlDataType = iota
	PL_DT_TRADE
	PL_DT_ORDERBOOK
	PL_DT_CANDLE
	PL_DT_ERROR
)
type PlMktCode struct {
	C1, C2 string
}
func NewPlMktCode(code string, exch PlExchange) PlMktCode {
	mktCode := PlMktCode {
		C1: "",
		C2: "",
	}
	if exch == PL_EXCH_BITHUMB || exch == PL_EXCH_UPBIT {
		parts := strings.Split(code, "-")
		mktCode.C1 = parts[0]
		mktCode.C2 = parts[1]
	}
	
	return mktCode
}
type PlDataTicker struct {
	Code PlMktCode
	TradePrice float64
	SignedChangePrice float64
	SignedChangeRate float64
	AccTradePrice float64
	AccTradePrice24h float64
	Timestamp int64
}
type PlDataTrade struct {
	Code PlMktCode
	TradeTimestamp int64
	TradePrice float64
	TradeVolume float64
	Timestamp int64
}
type PlDataOrderbookUnit = upbit.UpbitOrderbookUnit
type PlDataOrderbook struct {
	Code PlMktCode
	Timestamp int64
	TotalAskSize float64
	TotalBidSize float64
	OrderbookUnits []PlDataOrderbookUnit
}
type PlDataCandle struct {
	Code PlMktCode
	CandleDateTimeUTC string
	CandleDateTimeKST string
	OpeningPrice         float64
	HighPrice            float64
	LowPrice             float64
	TradePrice           float64
	CandleAccTradeVolume float64
	CandleAccTradePrice  float64
	Timestamp int64
}

type PlDataCheckpoint struct {
	Name string
	Ts time.Time
	Err error
}
type PlData struct {
	Exchange PlExchange
	DataType PlDataType
	Payload any
	CheckPoints []PlDataCheckpoint
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
