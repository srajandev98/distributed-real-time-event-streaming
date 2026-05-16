package types

// Message is the in-memory representation of one record in a partition log.
type Message struct {
	Offset int
	Value  string
}
