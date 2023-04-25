package communication

import (
	"sync"
)

const (
	MSG int = 0
	DLV int = 1
	STB int = 2
)

const (
	ADD int = 0
	REM int = 1
)

type Operation struct {
	Type    string // operation type
	Value   any    // value of the operation submitted by user
	Version VClock // vector clock kept for keeping causal order
}

// Compares two operations to see if they are concurrent
func (e *Operation) Concurrent(other *Operation) bool {
	if e.Version.Compare(other.Version) == Concurrent {
		return true
	}
	return e.Value == other.Value
}

type Message struct {
	Type      int    // type of message
	Operation        // operation submitted by user
	OriginID  string // replica which originally generated an message
}

// NewMessage creates a new message with the given value and version vector
func NewMessage(tp int, operation string, value any, version VClock, originID string) Message {
	return Message{
		Type:      tp,
		Operation: Operation{Type: operation, Value: value, Version: version},
		OriginID:  originID,
	}
}

// CompareTo compares two messages based on their version and timestamp.
// If the messages are concurrent, the one with the higher timestamp is considered to be newer.
func (e *Message) CompareTo(other *Message) Condition {
	return e.Version.Compare(other.Version)
}

// set type of message
func (e *Message) SetType(tp int) {
	e.Type = tp
}

// Check if two messages are equal by comparing their version, value, timestamp and origin
func (e *Message) Equals(other *Message) bool {
	return e.Version.Compare(other.Version) == Equal && e.Value == other.Value &&
		e.OriginID == other.OriginID
}

// creates new mutex for vector clock
func (e *Message) NewMutex() {
	e.Version.RWMutex = new(sync.RWMutex)
}
