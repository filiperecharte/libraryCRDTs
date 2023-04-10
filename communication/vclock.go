package communication

import (
	"sync"
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
type VClock struct {
	*sync.RWMutex
	m map[string]uint64
}

// FindTicks returns the clock value for a given id, if a value is not
// found false is returned
func (vc VClock) FindTicks(id string) uint64 {
	vc.Lock()
	ticks, _ := vc.m[id]
	vc.Unlock()
	return ticks
}

// New returns a new vector clock
func NewVClock() VClock {
	return VClock{
		RWMutex: new(sync.RWMutex),
		m:       make(map[string]uint64),
	}
}

func InitVClock(ids []string) VClock {
	vc := NewVClock()
	for _, i := range ids {
		vc.m[i] = 0
	}
	return vc
}

// Copy returns a copy of the clock
func (vc VClock) Copy() VClock {
	cp := NewVClock()
	vc.Lock()
	for key, value := range vc.m {
		cp.m[key] = value
	}
	vc.Unlock()
	return cp
}

// GetMap returns the map typed vector clock
func (vc VClock) GetMap() map[string]uint64 {
	vc.Lock()
	defer vc.Unlock()
	return map[string]uint64(vc.m)
}

// Set assigns a clock value to a clock index
func (vc VClock) Set(id string, ticks uint64) {
	vc.Lock()
	vc.m[id] = ticks
	vc.Unlock()
}

// Tick has replaced the old update
func (vc VClock) Tick(id string) {
	vc.Lock()
	vc.m[id] = vc.m[id] + 1
	vc.Unlock()
}

// Equal returns true if the callee's clock is equal to the other clock
func (vc VClock) Equals(other VClock) bool {
	vc.Lock()
	other.Lock()
	res := vc.Compare(other)
	vc.Unlock()
	other.Unlock()
	return res == Equal
}

// Compare takes another clock and determines if it is Equal,
// Ancestor, Descendant, or Concurrent with the callee's clock.
func (vc VClock) Compare(other VClock) Condition {
	var otherIs Condition
	vc.Lock()
	other.Lock()
	// Preliminary qualification based on length
	if len(vc.m) > len(other.m) {
		otherIs = Ancestor
	} else if len(vc.m) < len(other.m) {
		otherIs = Descendant
	} else {
		otherIs = Equal
	}

	// Compare matching items
	for id := range other.m {
		if _, found := vc.m[id]; found {
			if other.m[id] > vc.m[id] {
				switch otherIs {
				case Equal:
					otherIs = Descendant
					break
				case Ancestor:
					other.Unlock()
					vc.Unlock()
					return Concurrent
				}
			} else if other.m[id] < vc.m[id] {
				switch otherIs {
				case Equal:
					otherIs = Ancestor
					break
				case Descendant:
					other.Unlock()
					vc.Unlock()
					return Concurrent
				}
			}
		} else {
			if otherIs == Equal {
				other.Unlock()
				vc.Unlock()
				return Concurrent
			} else if len(other.m) <= len(vc.m) {
				other.Unlock()
				vc.Unlock()
				return Concurrent
			}
		}
	}

	for id := range vc.m {
		if _, found := other.m[id]; found {
			if other.m[id] > vc.m[id] {
				switch otherIs {
				case Equal:
					otherIs = Descendant
					break
				case Ancestor:
					other.Unlock()
					vc.Unlock()
					return Concurrent
				}
			} else if other.m[id] < vc.m[id] {
				switch otherIs {
				case Equal:
					otherIs = Ancestor
					break
				case Descendant:
					other.Unlock()
					vc.Unlock()
					return Concurrent
				}
			}
		} else {
			if otherIs == Equal {
				other.Unlock()
				vc.Unlock()
				return Concurrent
			} else if len(vc.m) <= len(other.m) {
				other.Unlock()
				vc.Unlock()
				return Concurrent
			}
		}
	}
	other.Unlock()
	vc.Unlock()
	return otherIs
}

// Subtract on vector clock from another
func (vc VClock) Subtract(vc1 VClock) (subVC VClock) {
	subVC = VClock{
		RWMutex: new(sync.RWMutex),
		m:       make(map[string]uint64),
	}
	vc.Lock()
	vc1.Lock()
	for key := range vc.m {
		subVC.m[key] = vc.m[key] - vc1.m[key]
	}
	vc1.Unlock()
	vc.Unlock()
	return subVC
}

// creates new mutex for vector clock
func (vc *VClock) NewMutex() {
	vc.RWMutex = new(sync.RWMutex)
}
