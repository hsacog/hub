package pipeline

type PlMetaType uint8
const (
	PL_MT_ERROR PlMetaType = iota
	PL_MT_METRIC
)
type PlMeta struct {
	MetaType PlMetaType
	Payload string
}
