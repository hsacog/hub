package upbit

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
)

/* UPBIT IF */
type UpbitIFDataOption struct {
	Type UpbitDT
	Option UpbitDTOption
}
type UpbitIFConfig struct {
	AccessKey string
	SecretKey string
	QuoUrl url.URL
	ExcUrl url.URL
	Options []UpbitIFDataOption
}
type UpbitIFState struct {
	validMktCodes map[string]struct{}
	subMktCodes map[string]struct{}
}
type UpbitIFUnitState uint8
const (
	READY UpbitIFUnitState = iota
	STOP
)

type UpbitIFUnitType uint8
const (
	QUOTATION UpbitIFUnitType = iota
	EXCHANGE
)

type UpbitIFUnit struct {
	Type UpbitIFUnitType
	conn *websocket.Conn
	ctl chan IFControl
	ctx *context.Context
	cancel *context.CancelFunc
	state UpbitIFUnitState
	lk sync.RWMutex
}

type UpbitIF struct {
	config UpbitIFConfig
	quoUnit UpbitIFUnit
	excUnit UpbitIFUnit
	pl chan UpbitRawData
	state UpbitIFState
	lk sync.RWMutex
}


/*
UPBIT IF CONTROL
*/

type IFControl struct {
	Type IFControlType
	Payload any
}

type IFControlType uint8
const (
	UPBIT_IF_PING = iota
	UPBIT_IF_WRITE
	UPBIT_IF_READ // do not control reading
	UPBIT_IF_STOP
	UPBIT_IF_RESET
)


/*
UPBIT REQUEST DATA TYPES
*/

type Payload struct {
	AccessKey string `json:"access_key"`
	Nonce string `json:"nonce"`
	QueryHash string `json:"query_hash,omitempty"`
	QueryHasnAlg string `json:"query_hash_alg,omitempty"`
	jwt.RegisteredClaims
}

type Ticket struct {
	Ticket string `json:"ticket"`
}
type DataType struct {
	Type string `json:"type"`
	Codes []string `json:"codes"`
	Level float64 `json:"level,omitempty"`
	IsOnlySnapshot bool `json:"is_only_snapshot,omitempty"`
	IsOnlyRealtime bool `json:"is_only_realtime,omitempty"`
}
type Format struct {
	Format string `json:"format"`
}

type UpbitDT uint8
const (
	UPBIT_TICKER UpbitDT = iota
	UPBIT_TRADE
	UPBIT_ORDERBOOK
	UPBIT_CANDLE
	UPBIT_ERROR
)

type UpbitDTOption struct {
	IsOnlySnapshot bool
	IsOnlyRealtime bool
	Level float64 // FOR UPBIT_ORDERBOOK
	OrderbookUnitSize int64 // FOR UPBIT_ORDERBOOK
	CandleInterval string // FOR UPBIT_CANDLE
}

/*
UPBIT RESPONSE DATA TYPES
*/
type UpbitMarketInfo struct {
	Market      string      `json:"market"`
	KoreanName  string      `json:"korean_name"`
	EnglishName string      `json:"english_name"`
	MarketEvent UpbitMarketEvent `json:"market_event"`
}

type UpbitMarketEvent struct {
	Warning bool    `json:"warning"`
	Caution UpbitMarketCaution `json:"caution"`
}

type UpbitMarketCaution struct {
	PriceFluctuations          bool `json:"PRICE_FLUCTUATIONS"`
	TradingVolumeSoaring       bool `json:"TRADING_VOLUME_SOARING"`
	DepositAmountSoaring       bool `json:"DEPOSIT_AMOUNT_SOARING"`
	GlobalPriceDifferences     bool `json:"GLOBAL_PRICE_DIFFERENCES"`
	ConcentrationOfSmallAccnts bool `json:"CONCENTRATION_OF_SMALL_ACCOUNTS"`
}



// for partial parsing
type UpbitHeader struct {
	Type                 string   `json:"type"`
}

type UpbitTicker struct {
	Type                 string   `json:"type"`
	Code                 string   `json:"code"`
	OpeningPrice         float64  `json:"opening_price"`
	HighPrice            float64  `json:"high_price"`
	LowPrice             float64  `json:"low_price"`
	TradePrice           float64  `json:"trade_price"`
	PrevClosingPrice     float64  `json:"prev_closing_price"`
	Change               string   `json:"change"`
	ChangePrice          float64  `json:"change_price"`
	SignedChangePrice    float64  `json:"signed_change_price"`
	ChangeRate           float64  `json:"change_rate"`
	SignedChangeRate     float64  `json:"signed_change_rate"`
	TradeVolume          float64  `json:"trade_volume"`
	AccTradeVolume       float64  `json:"acc_trade_volume"`
	AccTradeVolume24h    float64  `json:"acc_trade_volume_24h"`
	AccTradePrice 	 	 float64  `json:"acc_trade_price"`
	AccTradePrice24h     float64  `json:"acc_trade_price_24h"`
	TradeDate            string   `json:"trade_date"`
	TradeTime            string   `json:"trade_time"`
	TradeTimestamp       int64	  `json:"trade_timestamp"`
	AskBid               string   `json:"ask_bid"`
	AccAskVolume         float64  `json:"acc_ask_volume"`
	AccBidVolume         float64  `json:"acc_bid_volume"`
	Highest52WeekPrice   float64  `json:"highest_52_week_price"`
	Highest52WeekDate    string   `json:"highest_52_week_date"`
	Lowest52WeekPrice    float64  `json:"lowest_52_week_price"`
	Lowest52WeekDate     string   `json:"lowest_52_week_date"`
	MarketState          string   `json:"market_state"`
	DelistingDate        string	  `json:"delisting_date"`
	MarketWarning        string   `json:"market_warning"`
	Timestamp            int64    `json:"timestamp"`
	StreamType           string   `json:"stream_type"`
}

type UpbitTrade struct {
	Type              string  `json:"type"`
	Code              string  `json:"code"`
	Timestamp         int64  `json:"timestamp"`
	TradeDate         string  `json:"trade_date"`
	TradeTime         string  `json:"trade_time"`
	TradeTimestamp    int64  `json:"trade_timestamp"`
	TradePrice        float64 `json:"trade_price"`
	TradeVolume       float64 `json:"trade_volume"`
	AskBid            string  `json:"ask_bid"`
	PrevClosingPrice  float64 `json:"prev_closing_price"`
	Change            string  `json:"change"`
	ChangePrice       float64 `json:"change_price"`
	SequentialID      int64  `json:"sequential_id"`
	BestAskPrice      float64 `json:"best_ask_price"`
	BestAskSize       float64 `json:"best_ask_size"`
	BestBidPrice      float64 `json:"best_bid_price"`
	BestBidSize       float64 `json:"best_bid_size"`
	StreamType        string  `json:"stream_type"`
}

type UpbitOrderbook struct {
	Type           string          `json:"type"`
	Code           string          `json:"code"`
	Timestamp      int64         `json:"timestamp"`
	TotalAskSize   float64         `json:"total_ask_size"`
	TotalBidSize   float64         `json:"total_bid_size"`
	OrderbookUnits []UpbitOrderbookUnit `json:"orderbook_units"`
	StreamType     string          `json:"stream_type"`
	Level          float64         `json:"level"`
}

type UpbitOrderbookUnit struct {
	AskPrice float64 `json:"ask_price"`
	BidPrice float64 `json:"bid_price"`
	AskSize  float64 `json:"ask_size"`
	BidSize  float64 `json:"bid_size"`
}

type UpbitCandle struct {
	Type                 string  `json:"type"`
	Code                 string  `json:"code"`
	CandleDateTimeUTC    string  `json:"candle_date_time_utc"`
	CandleDateTimeKST    string  `json:"candle_date_time_kst"`
	OpeningPrice         float64 `json:"opening_price"`
	HighPrice            float64 `json:"high_price"`
	LowPrice             float64 `json:"low_price"`
	TradePrice           float64 `json:"trade_price"`
	CandleAccTradeVolume float64 `json:"candle_acc_trade_volume"`
	CandleAccTradePrice  float64 `json:"candle_acc_trade_price"`
	Timestamp            int64 `json:"timestamp"`
	StreamType           string  `json:"stream_type"`
}

type UpbitError struct {
	Error	UpbitErrorPayload `json:"error"`
}
type UpbitErrorPayload struct {
	Name string `json:"name"`
	Message string `json:"message"`
}

type UpbitRawData struct {
	Type UpbitDT
	Timestamp time.Time
	Bytes *[]byte
}
