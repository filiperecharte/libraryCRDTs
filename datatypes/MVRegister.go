package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type update struct {
	value   int
	version communication.VClock
}

type MVRegister struct {
	vstate []update // versioned state used to check concurrent operations
}

func (m *MVRegister) Apply(state any, operations []communication.Operation) any {
	st := []int{}

	// check if there are concurrent operations using vector clocks and join them in a set
	// if there are concurrent operations, then the set will have more than one element
	// if there are no concurrent operations, then the set will have only one element
	concurrent := []update{}

	for _, op := range operations {
		tmp := m.vstate[:0]
		for _, s := range m.vstate {
			msgOP := op

			cmp := msgOP.Version.Compare(s.version)
			if cmp == communication.Concurrent {
				concurrent = append(concurrent, s)
			}
			if cmp != communication.Ancestor {
				tmp = append(tmp, s)
			}
		}
		m.vstate = tmp
	}

	for _, op := range operations {
		msgOP := op
		m.vstate = append(m.vstate, update{msgOP.Value.(int), msgOP.Version})
		st = append(st, msgOP.Value.(int))
	}

	// if there are concurrent operations, then the set will have more than one element
	m.vstate = append(m.vstate, concurrent...)

	for _, s := range concurrent {
		st = append(st, s.value)
	}

	return st
}

// initialize counter replica
func NewMVRegisterReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	m := crdt.CommutativeCRDT{Data: &MVRegister{
		vstate: []update{},
	}, Stable_st: mapset.NewSet[int]()}

	return replica.NewReplica(id, &m, channels, delay)
}
