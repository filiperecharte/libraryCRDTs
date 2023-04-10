package middleware

import (
	"library/packages/communication"
	"sync"
)

// Matrix of vector clocks for each replica
type VClocks struct {
	sync.RWMutex
	m map[string]communication.VClock
}

// returns a new matrix of vector clocks
func InitVClocks(ids []string) VClocks {
	vc := VClocks{
		m: make(map[string]communication.VClock),
	}
	for _, id := range ids {
		vc.m[id] = communication.InitVClock(ids)
	}
	return vc
}

// returns vector clock that is common to all replicas by choosing the minimum value of each vector clock
func (vc VClocks) Common() communication.VClock {
	if len(vc.m) == 0 {
		return nil
	}
	common := make(communication.VClock)
	for _, vclock := range vc.m {
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
	if len(vc.m) == 0 {
		return nil
	}
	latest := make(communication.VClock)
	for _, vclock := range vc.m {
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
	for id, vclock := range other.m {
		if _, ok := vc.m[id]; !ok {
			vc.m[id] = make(communication.VClock)
		}
		vc.m[id].Merge(vclock)
	}
}

// updates the matrix by adding a new vector clock for a replica if it does not exist and merge if it exists
func (vc VClocks) Update(id string, vclock communication.VClock) {
	if _, ok := vc.m[id]; !ok {
		vc.m[id] = make(communication.VClock)
	}
	vc.m[id].Merge(vclock)
}
