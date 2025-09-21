package upbit

import (
	"context"
	"encoding/json"
	"errors"
	"hub/pkg/command"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

func NewUpbitIF(config UpbitIFConfig) (*UpbitIF, chan UpbitRawData) {
	ch := make(chan UpbitRawData, 8)
	return &UpbitIF{
		config: config,
		quoUnit: UpbitIFUnit{
			conn: nil,
			ctl: make(chan IFControl, 1000),
			ctx: nil,
			cancel: nil,
			state: STOP,
			seq: 0,
		},
		excUnit: UpbitIFUnit{
			conn: nil,
			ctl: make(chan IFControl, 1000),
			ctx: nil,
			cancel: nil,
			state: STOP,
			seq: 0,
		},
		pl: ch,	
		state: UpbitIFState{
			validMktCodes: make(map[string]struct{}),
			subMktCodes: make(map[string]struct{}),
		},
	}, ch
}

func (uif *UpbitIF) _open(unit *UpbitIFUnit) error {
	if unit.Type == QUOTATION {
		conn, res, err := websocket.DefaultDialer.Dial(uif.config.QuoUrl.String(), nil)
		if err != nil {
			log.Fatal(res)
			return err
		}
		unit.conn = conn
	} else if unit.Type == EXCHANGE {
		payload := Payload {
			AccessKey: uif.config.AccessKey,
			Nonce: uuid.New().String(),
		}
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS512, payload).SignedString([]byte(uif.config.SecretKey))
		header := http.Header{}
		header.Add("Authorization", "Bearer " + token)
		conn, res, err := websocket.DefaultDialer.Dial(uif.config.ExcUrl.String(), header)
		if err != nil {
			log.Fatal(res)
			return err
		}
		unit.conn = conn
	}

	// load valid mkt codes
	res, err := http.Get("https://api.upbit.com/v1/market/all")
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	
	var mktInfo []UpbitMarketInfo
	json.Unmarshal(body, &mktInfo)
	for _, info := range mktInfo {
		uif.state.validMktCodes[info.Market] = struct{}{}
	}
	
	return nil
}
func (uif *UpbitIF) _reset(unit *UpbitIFUnit) error {
	new_ctx, new_cancel := context.WithCancel(context.Background())
	unit.ctx = &new_ctx	
	unit.cancel = &new_cancel
	unit.seq += 1
	uif._open(unit)
	/* PING */
	uif._run_ws_ping(time.Second*10, unit, unit.seq)
	/* READ MESSAGE */
	uif._run_ws_reader(unit, unit.seq)
	/* RE-SUBSCRIBE */
	var currentCodes []string	
	for k := range uif.state.subMktCodes {
		currentCodes = append(currentCodes, k)
	}
	if len(currentCodes) != 0 {
		uif._subscribe(currentCodes)
	}
	return nil
}

func (uif *UpbitIF) _run_ws_ping(delay time.Duration, unit *UpbitIFUnit, seq int64) {
	ticker := time.NewTicker(delay)
	go func() {
		for {
			select {
			case <- ticker.C:
				unit.ctl <- IFControl{Type: UPBIT_IF_PING, Timestamp: time.Now(), Seq: seq}
			case <- (*unit.ctx).Done():
				log.Println("stop ping")
				return
			}
		}
	}()
	
}
func (uif *UpbitIF) _run_ws_reader(unit *UpbitIFUnit, seq int64) {
	go func() {
		for {
			_, data, err := unit.conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					log.Println("wss connection closed normally:", err)
				}
				if websocket.IsUnexpectedCloseError(err, websocket.CloseAbnormalClosure) {
					log.Println("wss connection closed abnormally:", err)
				}
				unit.ctl <- IFControl{Type: UPBIT_IF_STOP, Timestamp: time.Now(), Seq: seq}
				return
			} else {
				unit.ctl <- IFControl{Type: UPBIT_IF_READ, Payload: &data, Timestamp: time.Now(), Seq: seq}
			}
		}
	}()
}
func (uif *UpbitIF) _run_ws_main(unit *UpbitIFUnit) {
	/* synchronized processing for quoConn */
	go func() {
		for {
			select {
			case msg := <- unit.ctl:
				unit.lk.RLock()
				if msg.Seq != unit.seq {
					log.Printf("SKIP MSG_SEQ(%d) CUR_IF_SEQ(%d)", msg.Seq, unit.seq)
					continue
				}
				unit.lk.RUnlock()
				// log.Println("processing start")
				switch msg.Type {
				case UPBIT_IF_PING:
					unit.lk.RLock()
					if unit.state == READY {
						log.Println("send ping message")
						if err := (*unit.conn).WriteMessage(websocket.PingMessage, nil); err != nil {
							log.Println("ping error: ", err)
							// unit.ctl <- IFControl{Type: UPBIT_IF_STOP, Timestamp: time.Now()}
						}	
					}
					unit.lk.RUnlock()
				case UPBIT_IF_WRITE:
					unit.lk.RLock()
					data := msg.Payload.(*[]byte)
					if unit.state == READY {
						if err := (*unit.conn).WriteMessage(websocket.TextMessage, *data); err != nil {
							log.Fatal("write error: ", err)
							unit.ctl <- IFControl{Type: UPBIT_IF_STOP, Timestamp: time.Now(), Seq: unit.seq}
						}
					}
					unit.lk.RUnlock()
				case UPBIT_IF_READ:
					data := msg.Payload.(*[]byte)
					var header UpbitHeader
					var err error
					var plType UpbitDT
					if err = json.Unmarshal(*data, &header); err != nil || header.Type == "" {
						plType = UPBIT_ERROR
					} else {
						switch {
						case header.Type == "ticker":
							plType = UPBIT_TICKER
						case header.Type == "trade":
							plType = UPBIT_TRADE
						case header.Type == "orderbook":
							plType = UPBIT_ORDERBOOK
						case strings.HasPrefix(header.Type, "candle"):
							plType = UPBIT_CANDLE
						}
					}
					uif.pl <- UpbitRawData{Type: plType, ReceiveTimestamp: msg.Timestamp, Timestamp: time.Now(), Bytes: data}
				case UPBIT_IF_RESET:
					unit.lk.Lock()
					if unit.state != STOP {
						log.Println("Error: reset not in stop state")
					} else {
						log.Println("IF reset")
						uif._reset(unit)
						unit.state = READY
					}
					unit.lk.Unlock()
				case UPBIT_IF_STOP:
					unit.lk.Lock()
					if unit.state != READY {
						log.Println("Error: stop in ready state")
					} else {
						unit.state = STOP
						log.Println("IF stop")
						(*unit.cancel)()
						(unit.conn).Close()
						(unit.conn) = nil
						log.Println("IF reset")
						uif._reset(unit)
						unit.state = READY
					}
					unit.lk.Unlock()
				}
				// log.Println("processing end")
			}
		}	
	}()

}

func (uif *UpbitIF) Run() {
	uif._run_ws_main(&uif.quoUnit)
	uif.quoUnit.ctl <- IFControl{Type: UPBIT_IF_RESET, Timestamp: time.Now(), Seq: 0}
}

func (uif *UpbitIF) _subscribe(codes []string) error {
	log.Println("_subscribe", codes)
	msg := []any{}
	msg = append(msg,
		Ticket {
			Ticket: uuid.New().String(),
		},
	)
	for _, opt := range uif.config.Options {
		dataType := DataType {
			IsOnlySnapshot: opt.Option.IsOnlySnapshot,
			IsOnlyRealtime: opt.Option.IsOnlyRealtime,
		}
		switch opt.Type {
		case UPBIT_TICKER:
			dataType.Type = "ticker"
			dataType.Codes = codes
		case UPBIT_TRADE:
			dataType.Type = "trade"
			dataType.Codes = codes
		case UPBIT_ORDERBOOK:
			var orderCodes []string
			for _, code := range codes {
				orderCodes = append(orderCodes, code + "." + strconv.FormatInt(opt.Option.OrderbookUnitSize, 10))
			}
			dataType.Type = "orderbook"
			dataType.Codes = orderCodes
		case UPBIT_CANDLE:
			dataType.Type = "candle" + "." + opt.Option.CandleInterval
			dataType.Codes = codes
		}
		msg = append(msg, dataType)
	}
	msg = append(msg, 
		Format {
			Format: "DEFAULT",
		},
	)

	b, err := json.Marshal(msg)
	if err != nil {
		return err;
	}
	uif.quoUnit.lk.RLock()
	uif.quoUnit.ctl <- IFControl{Type: UPBIT_IF_WRITE, Payload: &b, Timestamp: time.Now(), Seq: uif.quoUnit.seq}
	uif.quoUnit.lk.RUnlock()
	return nil
} 

func (uif *UpbitIF) Subscribe(ps []command.MktPair) error {
	uif.lk.Lock()
	for _, p := range ps {
		cand1 := p.C1 + "-" + p.C2
		cand2 := p.C2 + "-" + p.C1
		var code string
		if _, ok := uif.state.validMktCodes[cand1]; ok {
			code = cand1	
		} else if _, ok := uif.state.validMktCodes[cand2]; ok {
			code = cand2	
		} else {
			return errors.New("mkt code not found")
		}

		uif.state.subMktCodes[code] = struct{}{}
	}
	var currentCodes []string	
	for k := range uif.state.subMktCodes {
		currentCodes = append(currentCodes, k)
	}
	
	uif._subscribe(currentCodes)
	uif.lk.Unlock()
	return nil
}
func (uif *UpbitIF) UnSubscribe(ps []command.MktPair) error {
	uif.lk.Lock()
	for _, p := range ps {
		cand1 := p.C1 + "-" + p.C2
		cand2 := p.C2 + "-" + p.C1
		var code string
		if _, ok := uif.state.validMktCodes[cand1]; ok {
			code = cand1	
		} else if _, ok := uif.state.validMktCodes[cand2]; ok {
			code = cand2	
		} else {
			return errors.New("mkt code not found")
		}
		if _, ok := uif.state.subMktCodes[code]; ok {
			delete(uif.state.subMktCodes, code)
		}
	}
	var currentCodes []string	
	for k := range uif.state.subMktCodes {
		currentCodes = append(currentCodes, k)
	}
	uif._subscribe(currentCodes)
	uif.lk.Unlock()
	return nil
}
