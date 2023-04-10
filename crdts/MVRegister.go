package crdts

import (
	"fmt"
	"library/packages/communication"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type update struct {
	value   int
	version *communication.VClock
}

type MVRegister struct {
	state mapset.Set[update]
}

func (r *MVRegister) TCDeliver(msg communication.Message) {
	// check if there are concurrent operations using vector clocks and join them in a set
	// if there are concurrent operations, then the set will have more than one element
	// if there are no concurrent operations, then the set will have only one element
	concurrent := mapset.NewSet[update]()

	for _, s := range r.state.ToSlice() {
		cmp := msg.Version.Compare(*s.version)
		if cmp == communication.Concurrent {
			concurrent.Add(s)
		} else if cmp == communication.Ancestor {
			r.state.Remove(s)
		}
	}

	// if there are concurrent operations, then the set will have more than one element
	r.state.Add(update{msg.Value.(int), &msg.Version})

	r.state.Union(concurrent)
}

func (r *MVRegister) TCStable(msg communication.Message) {
	fmt.Println("Ignoring received stable operation: ", msg)
}

func (r *MVRegister) Query() any {
	return r.state
}

// initialize counter
func NewMVRegister(id string, channels map[string]chan any) *replica.Replica {
	r := &MVRegister{
		state: mapset.NewSet[update](),
	}

	return replica.NewReplica(id, r, channels)
}
