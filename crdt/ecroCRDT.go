package crdt

import (
	"library/packages/communication"
	"log"
)

// data interface
type EcroDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// Order unstable operations.
	Order(operations []communication.Operation) []communication.Operation

	//Operations that commute
	Commutes(op1 communication.Operation, op2 communication.Operation) bool
}

type EcroCRDT struct {
	Id                  string
	Data                EcroDataI //data interface
	Stable_st           any       // stable state
	Unstable_operations []communication.Operation
	Unstable_st         any //most recent state
}

func (r *EcroCRDT) Effect(op communication.Operation) {
	// if r.after(op, r.Unstable_operations) {
	// 	r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{op})
	// 	r.Unstable_operations = append(r.Unstable_operations, op)
	// } else {
		r.Unstable_operations = append(r.Unstable_operations, op)
		//r.Unstable_st = r.Data.Apply(r.Stable_st, r.Data.Order(r.Unstable_operations))
	// }
}

func (r *EcroCRDT) Stabilize(op communication.Operation) {
	//remove op from slice
	// for i, o := range r.Unstable_operations {
	// 	if o.Type == op.Type && o.Value == op.Value {
	// 		r.Unstable_operations = append(r.Unstable_operations[:i], r.Unstable_operations[i+1:]...)
	// 		break
	// 	}
	// }

	// r.Data.Apply(r.Stable_st, []communication.Operation{op})
}

func (r *EcroCRDT) Query() any {
	log.Println("REPLICA", r.Id, "Unstable operations:", r.Unstable_operations)
	log.Println("REPLICA", r.Id, "Ordered operations:", r.Data.Order(r.Unstable_operations))
	return r.Unstable_st
}

func (r *EcroCRDT) after(op communication.Operation, operations []communication.Operation) bool {
	// commutes and order_after only with concurrent operations
	if r.commutes(op, operations) || r.order_after(op, operations) || r.causally_after(op, operations) {
		return true
	}
	return false
}

// checks if op commutes with all unstable concurrent operations
func (r *EcroCRDT) commutes(op communication.Operation, operations []communication.Operation) bool {
	for _, op2 := range operations {
		if op.Concurrent(op2) && !r.Data.Commutes(op, op2) {
			return false
		}
	}
	return true
}

func (r *EcroCRDT) order_after(op communication.Operation, operations []communication.Operation) bool {
	operations = append(operations, op)
	r.Data.Order(operations)
	op1 := operations[0]
	return op1.Type == op.Type && op1.Value == op.Value
}

func (r *EcroCRDT) causally_after(op communication.Operation, operations []communication.Operation) bool {
	for _, op2 := range operations {
		if op.Version.Compare(op2.Version) != communication.Ancestor {
			return false
		}
	}

	return true
}
