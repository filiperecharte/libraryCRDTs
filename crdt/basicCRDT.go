package crdt

import (
	"library/packages/communication"
)

// data interface
type BasicDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// Order unstable operations.
	Order(operations []communication.Operation) []communication.Operation
}

type BasicCRDT struct {
	Data                BasicDataI //data interface
	Stable_st           any        // stable state
	Unstable_operations []communication.Operation
}

func (r *BasicCRDT) Effect(op communication.Operation) {
	r.Unstable_operations = append(r.Unstable_operations, op)
}

func (r *BasicCRDT) Stabilize(op communication.Operation) {
	//remove op from slice
	for i, o := range r.Unstable_operations {
		if o.Type == op.Type && o.Value == op.Value {
			r.Unstable_operations = append(r.Unstable_operations[:i], r.Unstable_operations[i+1:]...)
			break
		}
	}
	r.Data.Apply(r.Stable_st, []communication.Operation{op})
}

func (r *BasicCRDT) Query() any {
	return r.Data.Apply(r.Stable_st, r.Data.Order(r.Unstable_operations))
}
