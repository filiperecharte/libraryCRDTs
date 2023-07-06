package communication

type Operation struct {
	Type     string // operation type
	Value    any    // value of the operation submitted by user
	Version  VClock // vector clock kept for keeping causal order
	OriginID string // replica which originally generated an operation
}

// Check if two operations are equal by comparing their version and type
func (e *Operation) Equals(other Operation) bool {
	return e.Version.Equal(other.Version) && e.Type == other.Type
}
