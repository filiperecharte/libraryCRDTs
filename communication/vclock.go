package communication

import (
	"bytes"
	"fmt"
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

// LastUpdate returns the clock value of the oldest clock
func (vc VClock) LastUpdate() (last uint64) {
	vc.Lock()
	for key := range vc.m {
		if vc.m[key] > last {
			last = vc.m[key]
		}
	}
	vc.Unlock()
	return last
}

// Merge takes the max of all clock values in other and updates the
// values of the callee
func (vc VClock) Merge(other VClock) {
	vc.Lock()
	other.Lock()
	for id := range other.m {
		if vc.m[id] < other.m[id] {
			vc.m[id] = other.m[id]
		}
	}
	vc.Unlock()
	other.Unlock()
}

// PrintVC prints the callee's vector clock to stdout
func (vc VClock) PrintVC() {
	fmt.Println(vc.ReturnVCString())
}

// ReturnVCString returns a string encoding of a vector clock
func (vc VClock) ReturnVCString() string {
	//sort
	vc.Lock()
	ids := make([]string, len(vc.m))
	i := 0
	for id := range vc.m {
		ids[i] = id
		i++
	}

	//sort.Strings(ids)

	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i := range ids {
		buffer.WriteString(fmt.Sprintf("\"%s\":%d", ids[i], vc.m[ids[i]]))
		if i+1 < len(ids) {
			buffer.WriteString(", ")
		}
	}
	vc.Unlock()
	buffer.WriteString("}")
	return buffer.String()
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
					return Concurrent
				}
			} else if other.m[id] < vc.m[id] {
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
			} else if len(other.m) <= len(vc.m) {
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
					return Concurrent
				}
			} else if other.m[id] < vc.m[id] {
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
			} else if len(vc.m) <= len(other.m) {
				return Concurrent
			}
		}
	}
	vc.Unlock()
	other.Unlock()

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
	vc.Unlock()
	vc1.Unlock()
	return subVC
}

// creates new mutex for vector clock
func (vc *VClock) NewMutex() {
	vc.RWMutex = new(sync.RWMutex)
}
