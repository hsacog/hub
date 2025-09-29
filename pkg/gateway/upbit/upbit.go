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
			resetCnt: 0,
			seq: 0,
		},
		excUnit: UpbitIFUnit{
			conn: nil,
			ctl: make(chan IFControl, 1000),
			ctx: nil,
			cancel: nil,
			state: STOP,
			resetCnt: 0,
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
			log.Println(res)
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
			log.Println(res)
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
func (uif *UpbitIF) _stop(unit *UpbitIFUnit) error {
	if unit.state != READY {
		log.Println("[_stop] not in READY state")
		return errors.New("[_stop] not in READY state")
	}
	log.Println("[_stop] IF stop")
	unit.state = STOP
	
	var err error
	(*unit.cancel)()
	// graceful shutdown
	// Send a WebSocket close message
    err = unit.conn.WriteControl(  
        websocket.CloseMessage,  
        websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),  
        time.Now().Add(time.Minute),  
    )  
    if err != nil {  
        return err  
    }  

    err = (*unit.conn).SetReadDeadline(time.Now().Add(5 * time.Second))  
    if err != nil {  
        return err  
    }  
    for {  
        _, _, err = unit.conn.NextReader()  
        if websocket.IsCloseError(err, websocket.CloseNormalClosure) {  
            break  
        }  
        if err != nil {  
            break  
        }  
    }

	err = (*unit.conn).SetWriteDeadline(time.Now().Add(5 * time.Second))
    err = unit.conn.Close()  
    if err != nil {  
        return err  
    }
	return nil
}
func (uif *UpbitIF) _reset(unit *UpbitIFUnit) error {
	if unit.state != STOP {
		log.Println("[_reset] not in STOP state")
		return errors.New("[_reset] not in STOP state")
	}
	log.Println("[_reset] IF reset")
	unit.resetCnt += 1;
	/* if unit.resetCnt >= 10 {
		return errors.New("reset count exceeded")
	} */
	new_ctx, new_cancel := context.WithCancel(context.Background())
	unit.ctx = &new_ctx	
	unit.cancel = &new_cancel
	unit.seq += 1
	if err := uif._open(unit); err != nil {
		return err;
	}
	/* MAIN */
	uif._run_ws_main(unit, unit.seq)
	/* PING */
	uif._run_ws_ping(time.Second*10, unit, unit.seq)
	/* READ MESSAGE */
	uif._run_ws_reader(unit, unit.seq)
	/* RE-SUBSCRIBE with delay */
	var currentCodes []string	
	for k := range uif.state.subMktCodes {
		currentCodes = append(currentCodes, k)
	}
	if len(currentCodes) != 0 {
		log.Println("re-subscribe after 5s: ", currentCodes)
		timer := time.After(5 * time.Second)
		<- timer
		uif._subscribe(currentCodes)
	}
	unit.state = READY
	return nil
}

func (uif *UpbitIF) _ping(unit *UpbitIFUnit) error {
	if unit.state != READY {
		log.Println("[_ping] not in READY state")
		return errors.New("[_ping] not in READY state")
	}
	log.Println("[_ping] send ping")
	(*unit.conn).SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := (*unit.conn).WriteMessage(websocket.PingMessage, nil); err != nil {
		log.Println("[_ping] ping failed: ", err)
		unit.ctl <- IFControl{
			Seq: unit.seq,
			Txn: []IFControlMsg{
				{Type: UPBIT_IF_STOP},
				{Type: UPBIT_IF_RESET},
			},
			Timestamp: time.Now(),
		}
	}	
	return nil
}

func (uif *UpbitIF) _write(unit *UpbitIFUnit, data *[]byte) error {
	if unit.state != READY {
		log.Println("[_write] not in READY state")
		return errors.New("[_write] not in READY state")
	}
	(*unit.conn).SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := (*unit.conn).WriteMessage(websocket.TextMessage, *data); err != nil {
		log.Println("[_write] write failed: ", err)
		unit.ctl <- IFControl{
			Seq: unit.seq,
			Txn: []IFControlMsg{
				{Type: UPBIT_IF_STOP},
				{Type: UPBIT_IF_RESET},
			},
			Timestamp: time.Now(),
		}
		return err
	}
	return nil
}

func (uif *UpbitIF) _read(unit *UpbitIFUnit, data *[]byte, rdt time.Time) error {
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
	uif.pl <- UpbitRawData{Type: plType, ReceiveTimestamp: rdt, Timestamp: time.Now(), Bytes: data}
	return nil
}

func (uif *UpbitIF) _subscribe(codes []string) error {
	log.Println("_subscribe"/* , codes */)
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
	// uif.quoUnit.lk.RLock()
	//log.Println("RLOCK(unit.quoUnit.lk)")
	uif.quoUnit.ctl <- IFControl{
		Seq: uif.quoUnit.seq,
		Txn: []IFControlMsg{
			{Type: UPBIT_IF_WRITE, Payload: &b},
		},
		Timestamp: time.Now(),
	}
	// uif.quoUnit.lk.RUnlock()
	//log.Println("RUNLOCK(unit.quoUnit.lk)")
	return nil
} 



func (uif *UpbitIF) _run_ws_ping(delay time.Duration, unit *UpbitIFUnit, seq int64) {
	ticker := time.NewTicker(delay)
	go func() {
		for {
			select {
			case <- ticker.C:
				unit.ctl <- IFControl{
					Seq: seq,
					Txn: []IFControlMsg{
						{Type: UPBIT_IF_PING},
					},
					Timestamp: time.Now(),
				}
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
				} else {
					log.Println("wss connection closed with error: ", err)
				}
				unit.ctl <- IFControl{
					Seq: unit.seq,
					Txn: []IFControlMsg{
						{Type: UPBIT_IF_STOP},
						{Type: UPBIT_IF_RESET},
					},
					Timestamp: time.Now(),
				}
				return
			} else {
				unit.ctl <- IFControl{
					Seq: seq,
					Txn: []IFControlMsg{
						{Type: UPBIT_IF_READ, Payload: &data},
					},
					Timestamp: time.Now(),
				}
			}
		}
	}()
}
func (uif *UpbitIF) _run_ws_main(unit *UpbitIFUnit, seq int64) {
	/* synchronized processing for quoConn */
	go func() {
		for {
			select {
			case ifc := <- unit.ctl:
				if ifc.Seq != seq {
					log.Printf("SKIP TXN_SEQ(%d) CUR_IF_SEQ(%d)", ifc.Seq, seq)
					continue
				}
				for _, msg := range ifc.Txn {
					switch msg.Type {
					case UPBIT_IF_PING:
						unit.lk.RLock()
						uif._ping(unit)
						unit.lk.RUnlock()
					case UPBIT_IF_WRITE:
						unit.lk.RLock()
						uif._write(unit, msg.Payload.(*[]byte))
						unit.lk.RUnlock()
					case UPBIT_IF_READ:
						uif._read(unit, msg.Payload.(*[]byte), ifc.Timestamp)
					case UPBIT_IF_RESET:
						unit.lk.Lock()
						if err := uif._reset(unit); err != nil {
							log.Fatal("[_run_ws_main] error: ", err)
						}
						unit.lk.Unlock()
						return // stop main (_reset starts new main)
					case UPBIT_IF_STOP:
						unit.lk.Lock()
						if err := uif._stop(unit); err != nil {
							log.Println("[_run_ws_main] error: ", err)
						}
						unit.lk.Unlock()
					}
				}
			}
		}	
	}()

}

func (uif *UpbitIF) Run() {
	uif._run_ws_main(&uif.quoUnit, 0)
	uif.quoUnit.ctl <- IFControl{
		Seq: 0,
		Txn: []IFControlMsg{
			{Type: UPBIT_IF_RESET},
		},
		Timestamp: time.Now(),
	}
}
func (uif *UpbitIF) Reset() error {
	uif.quoUnit.lk.RLock()	
	uif.quoUnit.ctl <- IFControl{
		Seq: uif.quoUnit.seq,
		Txn: []IFControlMsg{
			{Type: UPBIT_IF_STOP},
			{Type: UPBIT_IF_RESET},
		},
		Timestamp: time.Now(),
	}
	uif.quoUnit.lk.RUnlock()	
	return nil
}

func (uif *UpbitIF) Subscribe(ps []command.MktPair) error {
	uif.lk.Lock()
	uif.quoUnit.lk.RLock()
	// log.Println("LOCK(uif.lk)")
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
	uif.quoUnit.lk.RUnlock()
	uif.lk.Unlock()
	// log.Println("UNLOCK(uif.lk)")
	return nil
}
func (uif *UpbitIF) UnSubscribe(ps []command.MktPair) error {
	uif.lk.Lock()
	uif.quoUnit.lk.RLock()
	// log.Println("LOCK(uif.lk)")
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
	uif.quoUnit.lk.RUnlock()
	uif.lk.Unlock()
	// log.Println("UNLOCK(uif.lk)")
	return nil
}
