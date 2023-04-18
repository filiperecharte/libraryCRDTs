package crdt

import (
	"library/packages/communication"

	mapset "github.com/deckarep/golang-set/v2"
)

// all updates are reparable
type SemidirectDataI interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations mapset.Set[communication.Operation]) any

	// Repairs unstable operations.
	Repair(update communication.Operation, unstable_operations mapset.Set[communication.Operation]) communication.Operation
}

type SemidirectCRDT struct {
	Data                SemidirectDataI //data interface
	Unstable_operations mapset.Set[communication.Operation] //all aplied updates
	Unstable_st         any
}

func (r *SemidirectCRDT) Effect(op communication.Operation) {
	newUpdate := r.Data.Repair(op, r.Unstable_operations)
	r.Unstable_st = r.Data.Apply(r.Unstable_st, mapset.NewSet(newUpdate))
	r.Unstable_operations.Add(op)
}

func (r *SemidirectCRDT) Stabilize(op communication.Operation) {
	r.Unstable_operations.Remove(op)
}

func (r *SemidirectCRDT) Query() any {
	return r.Unstable_st
}
