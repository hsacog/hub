package api

type Response struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

type Ctl struct {
	Cmd string `json:"cmd"`
}

type Sub struct {
	Pairs []MktPair `json:"pairs"`
}
type MktPair struct {
	C1 string `json:"c1"`
	C2 string `json:"c2"`
}
