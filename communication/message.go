package communication

const (
	MSG int = 0
	DLV int = 1
	STB int = 2
)

type Message struct {
	Type     int         // type of message
	Value    interface{} // CRDT-specific operation submitted by user
	Version  VClock      // vector clock kept for keeping causal order
	OriginID string      // replica which originally generated an message
}

// NewMessage creates a new message with the given value and version vector
func NewMessage(tp int, value interface{}, version VClock, originID string) Message {
	return Message{
		Type:     tp,
		Value:    value,
		Version:  version,
		OriginID: originID,
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

// describe message in string format
func (e *Message) String() string {
	// improve string representation
	// TODO

	return "ORIGIN: " + string(e.OriginID) + " VCLOCK: " + e.Version.ReturnVCString()
}
