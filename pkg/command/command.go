package command

type CommandReceiver interface {
	Start() error
	Stop() error
}

type CommandType uint8
const (
	ADD_MKT CommandType = iota
	REMOVE_MKT
	RESET
)
type MktPair struct {
	C1 string
	C2 string
}
type Command struct {
	Type CommandType
	Payload []any
}

