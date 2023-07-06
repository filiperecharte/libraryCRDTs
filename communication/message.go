package communication

import (
	"sync"
)

const (
	MSG int = 0
	DLV int = 1
	STB int = 2
	EDG int = 3
)

type Message struct {
	MSGType   int // type of message
	Operation     // operation submitted by user
}

// NewMessage creates a new message with the given value and version vector
func NewMessage(tp int, operation string, value any, version VClock, originID string) Message {
	return Message{
		MSGType:   tp,
		Operation: Operation{Type: operation, Value: value, Version: version, OriginID: originID},
	}
}

// CompareTo compares two messages based on their version and timestamp.
// If the messages are concurrent, the one with the higher timestamp is considered to be newer.
func (e *Message) CompareTo(other *Message) Condition {
	return e.Version.Compare(other.Version)
}

// set type of message
func (e *Message) SetType(tp int) {
	e.MSGType = tp
}

// Check if two messages are equal by comparing their version, value, timestamp and origin
func (e *Message) Equals(other *Message) bool {
	return e.Version.Compare(other.Version) == Equal && e.Value == other.Value &&
		e.OriginID == other.OriginID
}

// creates new mutex for vector clock
func (e *Message) NewMutex() {
	e.Operation.Version.RWMutex = new(sync.RWMutex)
}
