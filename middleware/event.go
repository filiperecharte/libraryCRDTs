package middleware

import (
	"time"
)

type Event struct {
	Value     interface{} // CRDT-specific operation submitted by user
	Version   VClock      // vector clock kept for keeping causal order
	Timestamp time.Time   // wall clock timestamp - useful for LWW and total ordering in case of concurrent versions
	Origin    ReplicaID   // replica which originally generated an event
}

// CompareTo compares two events based on their version and timestamp.
// If the events are concurrent, the one with the higher timestamp is considered to be newer.
func (e *Event) CompareTo(other *Event) Condition {
	cmp := e.Version.Compare(other.Version)
	if cmp == Concurrent {
		cmp := e.Timestamp.After(other.Timestamp)
		if cmp {
			return Descendant
		} else if e.Timestamp.Before(other.Timestamp) {
			return Ancestor
		}
		return Equal
	}
	return Descendant
}

// Check if two events are equal by comparing their version, value, timestamp and origin
func (e *Event) Equals(other *Event) bool {
	return e.Version.Compare(other.Version) == Equal && e.Value == other.Value &&
		e.Timestamp == other.Timestamp && e.Origin == other.Origin
}

// describe event in string format
func (e *Event) String() string {
	return "ORIGIN: " + string(e.Origin) + " VCLOCK: " + e.Version.ReturnVCString() + " TIMESTAMP: " + e.Timestamp.String()
}
