package middleware

import (
	"library/packages/crdt"
	"time"
)

// Matrix of vector clocks for each replica
type VClocks map[string]VClock

// returns vector clock that is common to all replicas by choosing the minimum value of each vector clock
func (vc VClocks) Common() VClock {
	if len(vc) == 0 {
		return nil
	}
	common := make(VClock)
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
func (vc VClocks) Latest() VClock {
	if len(vc) == 0 {
		return nil
	}
	latest := make(VClock)
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
			vc[id] = make(VClock)
		}
		vc[id].Merge(vclock)
	}
}

// updates the matrix by adding a new vector clock for a replica if it does not exist and merge if it exists
func (vc VClocks) Update(id string, vclock VClock) {
	if _, ok := vc[id]; !ok {
		vc[id] = make(VClock)
	}
	vc[id].Merge(vclock)
}

type Middleware struct {
	Unstable      map[interface{}]struct{} // Event operations waiting to stabilize
	StableVersion VClock                   // last known stable timestamp
	LatestVersion VClock                   // most up-to-date vector timestamp
	Observed      VClocks                  // vector versions of observed universe
}

// creates middleware state
func NewMiddleware() *Middleware {
	return &Middleware{
		Unstable:      make(map[interface{}]struct{}),
		StableVersion: make(VClock),
		LatestVersion: make(VClock),
		Observed:      make(VClocks),
	}
}

func stabilize(crdt crdt.Crdt, state *Middleware) (stableVClock VClock) {
	stableVClock = state.Observed.Common()
	stable := make(map[interface{}]struct{})
	unstable := make(map[interface{}]struct{})

	for op, _ := range state.Unstable {
		cmp := op.(Event).Version.Compare(stableVClock)
		if cmp == Equal || cmp == Ancestor {
			stable[op] = struct{}{}
		} else {
			unstable[op] = struct{}{}
		}
	}

	// delete stable operations from unstable state
	// TODO

	//send stable operations to replica maybe using INFORM function
	// TODO
	return
}

// prints middleware state
func (state Middleware) String() string {
	//improve string representation
	// TODO
	return "StableVersion: " + state.StableVersion.ReturnVCString() + "\n" +
		"LatestVersion: " + state.LatestVersion.ReturnVCString() + "\n"
}

//-------------------- Protocol --------------------//

// SUBMIT operation that creates a new event with an incremented version, adds the event to unstable state
// and sends it to all replicas.
func Update(replicaID string, state *Middleware, operation interface{}) {
	// increment version
	state.LatestVersion.Tick(replicaID)

	//increment observed version
	state.Observed.Update(replicaID, state.LatestVersion)

	//creates an event
	event := new(Event)
	event.Origin = replicaID
	event.Version = state.LatestVersion
	event.Timestamp = time.Now()
	event.Value = operation

	// add event to unstable state
	state.Unstable[event] = struct{}{}

	// broadcast event to all replicas
	// TODO

	// call stabilize to see if there are any stable operations
	// TODO

	// do the previous two steps concurrently
	// TODO
}

// INFORM is a callback that will be called to inform that the replica has more stable operations to apply.
func Inform(state Middleware) {
	// TODO
}

// SEND operation to a given replica
func Send(state Middleware, replica string, operation interface{}) {
	// TODO
}

// RECEIVE operations from replicas
func Receive() {
	// TODO
}
