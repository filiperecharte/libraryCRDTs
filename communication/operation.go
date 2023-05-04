package communication

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
func (e *Operation) Concurrent(other Operation) bool {
	if e.Version.Compare(other.Version) == Concurrent && e.Value == other.Value {
		return true
	}
	return false
}

// Check if two operations are equal by comparing their version, value and type
func (e *Operation) Equals(other Operation) bool {
	return e.Version.Equal(other.Version) && e.Value == other.Value && e.Type == other.Type
}
