package kafka

import (
	"encoding/json"
	. "hub/pkg/pipeline"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// not supports SASL/PLAIN
type ProduceUnitConfig struct {
	Brokers []string	
	Id string
	Pw string
}

type Topic1 struct {
	Code []string `json:"mkt_code"`
	Exchange string `json:"exchange"`
	TradePrice float64 `json:"trade_price"`
	SignedChangePrice float64 `json:"signed_change_price"`
	SignedChangeRate float64 `json:"signed_change_rate"`
	Timestamp int64 `json:"timestamp"`
}

type Topic2 struct {
	Code []string  `json:"mkt_code"`
	Exchange string `json:"exchange"`
	CandleDateTimeUTC string  `json:"candle_date_time_utc"`
	CandleDateTimeKST string  `json:"candle_date_time_kst"`
	OpeningPrice float64 `json:"opening_price"`
	HighPrice float64 `json:"high_price"`
	LowPrice float64 `json:"low_price"`
	TradePrice float64 `json:"trade_price"`
	CandleAccTradeVolume float64 `json:"candle_acc_trade_volume"`
	CandleAccTradePrice float64 `json:"candle_acc_trade_price"`
	Timestamp int64 `json:"timestamp"`
}

type Topic3 struct {
	Code []string `json:"mkt_code"`
	Exchange string `json:"exchange"`
	Timestamp int64 `json:"timestamp"`
	TotalAskSize float64 `json:"total_ask_size"`
	TotalBidSize float64 `json:"total_bid_size"`
	OrderbookUnits []PlDataOrderbookUnit
}

type Topic4 struct {
	Code []string `json:"mkt_code"`
	Exchange string `json:"exchange"`
	TradePrice float64 `json:"trade_price"`
	AccTradePrice float64 `json:"acc_trade_price"`
	AccTradePrice24h float64 `json:"acc_trade_price_24h"`
	Timestamp int64 `json:"timestamp"`
}

type Topic uint8
const (
	TICKER_BASIC Topic = iota
	TICKER_EXTENDED
	ORDERBOOK_5
	CANDLE_1S
)
var TopicMap = map[Topic]string{
	TICKER_BASIC: "ticker-basic",
	TICKER_EXTENDED: "ticker-extended",
	ORDERBOOK_5: "orderbook-5",
	CANDLE_1S: "candel-1s",
}

func GetPartition(c string) int32 {
	switch (c) {
	case "ETH":
		return 0
	case "XRP":
		return 1
	case "BTC":
		return 2
	case "DOGE":
		return 3
	case "SOL":
		return 4
	case "ZKC":
		return 5
	case "ADA":
		return 6
	case "SUI":
		return 7
	case "LINK":
		return 8
	default:
		return 9
	}
}

func ProducePipeline(config ProduceUnitConfig) (*Pipeline[*PlData, *PlData], error) {
	cf := sarama.NewConfig()
	cf.Producer.RequiredAcks = sarama.NoResponse
	cf.Producer.Compression = sarama.CompressionSnappy
	cf.Producer.Flush.Frequency = 1 * time.Millisecond
	cf.Producer.Partitioner = sarama.NewManualPartitioner

	log.Println("start creating producer", config)
	producer, err := sarama.NewAsyncProducer(config.Brokers, cf)
	if err != nil {
		return nil, err
	}
	log.Println("end creating producer")

	go func() {
		for err := range producer.Errors() {
			log.Println("Produce kafka msg error:", err)
		}
	}()
	
	return NewPipeline(1000, func (data *PlData) *PlData {
		data.CheckPoints = append(data.CheckPoints, PlDataCheckpoint{Name: "pd", Ts: time.Now(), Err: nil})
		var err error = nil
		var msg []byte
		var value sarama.ByteEncoder
		var pm *sarama.ProducerMessage
		if data.DataType == PL_DT_TICKER {
			payload := data.Payload.(PlDataTicker)
			msg, err = json.Marshal(Topic1 {
				Code: []string{payload.Code.C1, payload.Code.C2},
				Exchange: PlExchangeMap[data.Exchange],
				TradePrice: payload.TradePrice,
				SignedChangePrice: payload.SignedChangePrice,
				SignedChangeRate: payload.SignedChangeRate,	
				Timestamp: payload.Timestamp,
			})
			if err != nil {
				goto RETURN
			}
			
			if value, err = sarama.ByteEncoder.Encode(msg); err != nil {
				goto RETURN
			}
			pm = &sarama.ProducerMessage{Topic: TopicMap[TICKER_BASIC], Value: value, Partition: GetPartition(payload.Code.C2)}
			/* partition, offset, err := producer.SendMessage(pm)
			log.Println(partition, offset, err) */
			producer.Input() <- pm

			msg, err = json.Marshal(Topic4 {
				Code: []string{payload.Code.C1, payload.Code.C2},
				Exchange: PlExchangeMap[data.Exchange],
				AccTradePrice: payload.AccTradePrice,
				AccTradePrice24h: payload.AccTradePrice24h,
				Timestamp: payload.Timestamp,
			})
			if err != nil {
				goto RETURN
			}
			if value, err = sarama.ByteEncoder.Encode(msg); err != nil {
				goto RETURN
			}
			pm = &sarama.ProducerMessage{Topic: TopicMap[TICKER_EXTENDED], Value: value, Partition: GetPartition(payload.Code.C2)}
			// partition, offset, err = producer.SendMessage(pm)
			// log.Println(partition, offset, err)
			producer.Input() <- pm

		} else if data.DataType == PL_DT_ORDERBOOK {
			payload := data.Payload.(PlDataOrderbook)
			msg, err = json.Marshal(Topic3 {
				Code: []string{payload.Code.C1, payload.Code.C2},
				Exchange: PlExchangeMap[data.Exchange],
				TotalAskSize: payload.TotalAskSize,
				TotalBidSize: payload.TotalBidSize,
				OrderbookUnits: payload.OrderbookUnits,
				Timestamp: payload.Timestamp,
			})
			if err != nil {
				goto RETURN
			}
			if value, err = sarama.ByteEncoder.Encode(msg); err != nil {
				goto RETURN
			}
			pm = &sarama.ProducerMessage{Topic: TopicMap[ORDERBOOK_5], Value: value, Partition: GetPartition(payload.Code.C2)}
			// partition, offset, err := producer.SendMessage(pm)
			// log.Println(partition, offset, err)
			producer.Input() <- pm

		} else if data.DataType == PL_DT_CANDLE {
			payload := data.Payload.(PlDataCandle)
			msg, err = json.Marshal(Topic2 {
				Code: []string{payload.Code.C1, payload.Code.C2},
				Exchange: PlExchangeMap[data.Exchange],
				CandleDateTimeUTC: payload.CandleDateTimeUTC,
				CandleDateTimeKST: payload.CandleDateTimeKST,
				OpeningPrice: payload.OpeningPrice,
				HighPrice: payload.HighPrice,
				LowPrice: payload.LowPrice,
				TradePrice: payload.TradePrice,
				CandleAccTradeVolume: payload.CandleAccTradeVolume,
				CandleAccTradePrice: payload.CandleAccTradePrice,
				Timestamp: payload.Timestamp,
			})
			if err != nil {
				goto RETURN
			}
			if value, err = sarama.ByteEncoder.Encode(msg); err != nil {
				goto RETURN
			}
			pm = &sarama.ProducerMessage{Topic: TopicMap[CANDLE_1S], Value: value, Partition: GetPartition(payload.Code.C2)}
			// partition, offset, err := producer.SendMessage(pm)
			// log.Println(partition, offset, err)
			producer.Input() <- pm
		}
RETURN:
		return data	
	}), nil
}

