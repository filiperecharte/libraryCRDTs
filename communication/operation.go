package communication

type Operation struct {
	Type    string // operation type
	Value   any    // value of the operation submitted by user
	Version VClock // vector clock kept for keeping causal order
	OriginID  string // replica which originally generated an operation
}

// Compares two operations to see if they are concurrent
func (e *Operation) Concurrent(other Operation) bool {
	return e.Version.Compare(other.Version) == Concurrent
}

// Check if two operations are equal by comparing their version, value and type
func (e *Operation) Equals(other Operation) bool {
	return e.Version.Equal(other.Version) && e.Type == other.Type
}
