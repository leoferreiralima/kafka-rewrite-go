package protocol

type TaggedField struct {
	Tag  uint32 `kafka:"0"`
	Data []byte `kafka:"1"`
}
