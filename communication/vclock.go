package communication

import (
	"bytes"
	"fmt"
)

// Condition constants define how to compare a vector clock against another,
// and may be ORed together when being provided to the Compare method.
type Condition int

// Constants define comparison conditions between pairs of vector
// clocks
const (
	Equal Condition = 1 << iota
	Ancestor
	Descendant
	Concurrent
)

// VClock are maps of string to uint64 where the string is the
// id of the process, and the uint64 is the clock value
type VClock map[string]uint64

// FindTicks returns the clock value for a given id, if a value is not
// found false is returned
func (vc VClock) FindTicks(id string) (uint64, bool) {
	ticks, ok := vc[id]
	return ticks, ok
}

// New returns a new vector clock
func NewVClock() VClock {
	return VClock{}
}

func InitVClock(ids []string) VClock {
	vc := NewVClock()
	for _, i := range ids {
		vc[i] = 0
	}
	return vc
}

// Copy returns a copy of the clock
func (vc VClock) Copy() VClock {
	cp := make(map[string]uint64, len(vc))
	for key, value := range vc {
		cp[key] = value
	}
	return cp
}

// CopyFromMap copies a map to a vector clock
func (vc VClock) CopyFromMap(otherMap map[string]uint64) VClock {
	return otherMap
}

// GetMap returns the map typed vector clock
func (vc VClock) GetMap() map[string]uint64 {
	return map[string]uint64(vc)
}

// Set assigns a clock value to a clock index
func (vc VClock) Set(id string, ticks uint64) {
	vc[id] = ticks
}

// Tick has replaced the old update
func (vc VClock) Tick(id string) {
	vc[id] = vc[id] + 1
}

// LastUpdate returns the clock value of the oldest clock
func (vc VClock) LastUpdate() (last uint64) {
	for key := range vc {
		if vc[key] > last {
			last = vc[key]
		}
	}
	return last
}

// Merge takes the max of all clock values in other and updates the
// values of the callee
func (vc VClock) Merge(other VClock) {
	for id := range other {
		if vc[id] < other[id] {
			vc[id] = other[id]
		}
	}
}

// PrintVC prints the callee's vector clock to stdout
func (vc VClock) PrintVC() {
	fmt.Println(vc.ReturnVCString())
}

// ReturnVCString returns a string encoding of a vector clock
func (vc VClock) ReturnVCString() string {
	//sort
	ids := make([]string, len(vc))
	i := 0
	for id := range vc {
		ids[i] = id
		i++
	}

	//sort.Strings(ids)

	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i := range ids {
		buffer.WriteString(fmt.Sprintf("\"%s\":%d", ids[i], vc[ids[i]]))
		if i+1 < len(ids) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString("}")
	return buffer.String()
}

// Equal returns true if the callee's clock is equal to the other clock
func (vc VClock) Equals(other VClock) bool {
	return vc.Compare(other) == Equal
}

// Compare takes another clock and determines if it is Equal,
// Ancestor, Descendant, or Concurrent with the callee's clock.
func (vc VClock) Compare(other VClock) Condition {
	var otherIs Condition
	// Preliminary qualification based on length
	if len(vc) > len(other) {
		otherIs = Ancestor
	} else if len(vc) < len(other) {
		otherIs = Descendant
	} else {
		otherIs = Equal
	}

	// Compare matching items
	for id := range other {
		if _, found := vc[id]; found {
			if other[id] > vc[id] {
				switch otherIs {
				case Equal:
					otherIs = Descendant
					break
				case Ancestor:
					return Concurrent
				}
			} else if other[id] < vc[id] {
				switch otherIs {
				case Equal:
					otherIs = Ancestor
					break
				case Descendant:
					return Concurrent
				}
			}
		} else {
			if otherIs == Equal {
				return Concurrent
			} else if len(other) <= len(vc) {
				return Concurrent
			}
		}
	}

	for id := range vc {
		if _, found := other[id]; found {
			if other[id] > vc[id] {
				switch otherIs {
				case Equal:
					otherIs = Descendant
					break
				case Ancestor:
					return Concurrent
				}
			} else if other[id] < vc[id] {
				switch otherIs {
				case Equal:
					otherIs = Ancestor
					break
				case Descendant:
					return Concurrent
				}
			}
		} else {
			if otherIs == Equal {
				return Concurrent
			} else if len(vc) <= len(other) {
				return Concurrent
			}
		}
	}

	return otherIs
}

// Subtract on vector clock from another
func (vc VClock) Subtract(vc1 VClock) (subVC VClock) {
	subVC = make(map[string]uint64)
	for key := range vc {
		subVC[key] = vc[key] - vc1[key]
	}
	return subVC
}
