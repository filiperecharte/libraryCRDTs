package crdt

import (
	"library/packages/communication"

	mapset "github.com/deckarep/golang-set/v2"
)

// all updates are reparable
type SemidirectDataI interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations mapset.Set[any]) any

	// Repairs unstable operations.
	Repair(update any, unstable_operations mapset.Set[any]) any
}

type SemidirectCRDT struct {
	Data                SemidirectDataI //data interface
	Unstable_operations mapset.Set[any] //all aplied updates
	Unstable_st         any
}

func (r *SemidirectCRDT) TCDeliver(msg communication.Message) {
	newUpdate := r.Data.Repair(msg.Value, r.Unstable_operations)
	r.Unstable_st = r.Data.Apply(r.Unstable_st, mapset.NewSet(newUpdate))
	r.Unstable_operations.Add(msg.Value)
}

func (r *SemidirectCRDT) TCStable(msg communication.Message) {
	r.Unstable_operations.Remove(msg.Value)
}

func (r *SemidirectCRDT) Query() any {
	return r.Unstable_st
}
