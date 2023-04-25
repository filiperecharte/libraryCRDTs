package crdt

import (
	"library/packages/communication"
)

// all updates are reparable
type SemidirectDataI interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// Repairs unstable operations.
	Repair(update communication.Operation, unstable_operations []communication.Operation) communication.Operation
}

type SemidirectCRDT struct {
	Data                SemidirectDataI           //data interface
	Unstable_operations []communication.Operation //all aplied updates
	Unstable_st         any
}

func (r *SemidirectCRDT) Effect(op communication.Operation) {
	newUpdate := r.Data.Repair(op, r.Unstable_operations)
	r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{newUpdate})
	r.Unstable_operations = append(r.Unstable_operations, newUpdate)
}

func (r *SemidirectCRDT) Stabilize(op communication.Operation) {
	for i, o := range r.Unstable_operations {
		if o.Type == op.Type && o.Value == op.Value {
			r.Unstable_operations = append(r.Unstable_operations[:i], r.Unstable_operations[i+1:]...)
			break
		}
	}
}

func (r *SemidirectCRDT) Query() any {
	return r.Unstable_st
}
