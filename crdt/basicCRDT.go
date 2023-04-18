package crdt

import (
	"library/packages/communication"

	mapset "github.com/deckarep/golang-set/v2"
)

// data interface
type BasicDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations mapset.Set[communication.Operation]) any

	// Order unstable operations.
	Order(operations mapset.Set[communication.Operation]) mapset.Set[communication.Operation]
}

type BasicCRDT struct {
	Data                BasicDataI //data interface
	Stable_st           any        // stable state
	Unstable_operations mapset.Set[communication.Operation]
}

func (r *BasicCRDT) Effect(op communication.Operation) {
	r.Unstable_operations.Add(op)
}

func (r *BasicCRDT) Stabilize(op communication.Operation) {
	r.Unstable_operations.Remove(op)
	r.Data.Apply(r.Stable_st, mapset.NewSet(op))
}

func (r *BasicCRDT) Query() any {
	return r.Data.Apply(r.Stable_st, r.Data.Order(r.Unstable_operations))
}
