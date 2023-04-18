package crdt

import (
	"library/packages/communication"

	mapset "github.com/deckarep/golang-set/v2"
)

// data interface
type BasicDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations mapset.Set[any]) any

	// Order unstable operations.
	Order(operations mapset.Set[any]) mapset.Set[any]

	//Operations that commute
	Commutes(op1 any, op2 any) bool
}

type BasicCRDT struct {
	Data                BasicDataI //data interface
	Stable_st           any        // stable state
	Unstable_operations mapset.Set[any]
}

func (r *BasicCRDT) Effect(msg communication.Message) {
	r.Unstable_operations.Add(msg.Value)
}

func (r *BasicCRDT) Stabilize(msg communication.Message) {
	r.Unstable_operations.Remove(msg.Value)
	r.Data.Apply(r.Stable_st, mapset.NewSet(msg.Value))
}

func (r *BasicCRDT) Query() any {
	return r.Data.Apply(r.Stable_st, r.Data.Order(r.Unstable_operations))
}
