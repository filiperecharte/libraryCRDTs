package middleware

import (
	"library/packages/communication"
	"sync"
)

// Matrix of vector clocks for each replica
type VClocks struct {
	*sync.RWMutex
	m map[string]communication.VClock
}

// returns a new matrix of vector clocks
func InitVClocks(ids []string) VClocks {
	vc := VClocks{
		RWMutex: new(sync.RWMutex),
		m:       make(map[string]communication.VClock),
	}
	for _, id := range ids {
		vc.m[id] = communication.InitVClock(ids)
	}
	return vc
}

// returns map for a specific position
func (vcs VClocks) GetTick(id string, id1 string) uint64 {
	vcs.Lock()
	defer vcs.Unlock()
	return vcs.m[id].FindTicks(id1)
}

// set vclock for a specific position
func (vcs *VClocks) SetVClock(id string, vc communication.VClock) {
	vcs.Lock()
	vcs.m[id] = vc
	vcs.Unlock()
}

// returns map
func (vcs VClocks) GetMap() map[string]communication.VClock {
	vcs.Lock()
	defer vcs.Unlock()
	return map[string]communication.VClock(vcs.m)
}
