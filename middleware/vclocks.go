package middleware

import (
	"library/packages/communication"
)

// Matrix of vector clocks for each replica
type VClocks map[string]communication.VClock

// returns a new matrix of vector clocks
func InitVClocks(ids []string) VClocks {
	vc := make(VClocks)
	for _, id := range ids {
		vc[id] = communication.InitVClock(ids)
	}
	return vc
}

// returns vector clock that is common to all replicas by choosing the minimum value of each vector clock
func (vc VClocks) Common() communication.VClock {
	if len(vc) == 0 {
		return nil
	}
	common := make(communication.VClock)
	for _, vclock := range vc {
		for id, ticks := range vclock {
			if common[id] > ticks {
				common[id] = ticks
			}
		}
	}
	return common
}

// returns the latest vector clock (most up to date) by choosing the maximum value of each vector clock
func (vc VClocks) Latest() communication.VClock {
	if len(vc) == 0 {
		return nil
	}
	latest := make(communication.VClock)
	for _, vclock := range vc {
		for id, ticks := range vclock {
			if latest[id] < ticks {
				latest[id] = ticks
			}
		}
	}
	return latest
}

// merges two matrix of vector clocks together by choosing the maximum value of each vector clock
func (vc VClocks) Merge(other VClocks) {
	for id, vclock := range other {
		if _, ok := vc[id]; !ok {
			vc[id] = make(communication.VClock)
		}
		vc[id].Merge(vclock)
	}
}

// updates the matrix by adding a new vector clock for a replica if it does not exist and merge if it exists
func (vc VClocks) Update(id string, vclock communication.VClock) {
	if _, ok := vc[id]; !ok {
		vc[id] = make(communication.VClock)
	}
	vc[id].Merge(vclock)
}
