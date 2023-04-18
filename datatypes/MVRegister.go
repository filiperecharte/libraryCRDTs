package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type update struct {
	value   int
	version *communication.VClock
}

type MVRegister struct {
	vstate mapset.Set[update] // versioned state used to check concurrent operations
}

func (m *MVRegister) Apply(state any, operations []communication.Operation) any {
	st := mapset.NewSet[int]()

	// check if there are concurrent operations using vector clocks and join them in a set
	// if there are concurrent operations, then the set will have more than one element
	// if there are no concurrent operations, then the set will have only one element
	concurrent := mapset.NewSet[update]()

	for _, op := range operations {
		for _, s := range m.vstate.ToSlice() {
			msgOP := op

			cmp := msgOP.Version.Compare(*s.version)
			if cmp == communication.Concurrent {
				concurrent.Add(s)
			} else if cmp == communication.Ancestor {
				m.vstate.Remove(s)

			}
		}
	}

	for _, op := range operations {
		msgOP := op
		m.vstate.Add(update{msgOP.Value.(int), msgOP.Version})
		st.Add(msgOP.Value.(int))
	}

	// if there are concurrent operations, then the set will have more than one element
	m.vstate.Union(concurrent)

	for _, s := range concurrent.ToSlice() {
		st.Add(s.value)
	}

	return st
}

// initialize counter replica
func NewMVRegisterReplica(id string, channels map[string]chan any) *replica.Replica {

	m := crdt.CommutativeCRDT{Data: &MVRegister{
		vstate: mapset.NewSet[update](),
	}, Stable_st: mapset.NewSet[int]()}

	return replica.NewReplica(id, &m, channels)
}
