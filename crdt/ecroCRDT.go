package crdt

import (
	"library/packages/communication"

	mapset "github.com/deckarep/golang-set/v2"
)

// data interface
type EcroDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations mapset.Set[communication.Operation]) any

	// Order unstable operations.
	Order(operations mapset.Set[communication.Operation]) mapset.Set[communication.Operation]

	//Operations that commute
	Commutes(op1 communication.Operation, op2 communication.Operation) bool
}

type EcroCRDT struct {
	Data                EcroDataI //data interface
	Stable_st           any       // stable state
	Unstable_operations mapset.Set[communication.Operation]
	Unstable_st         any //most recent state
}

func (r *EcroCRDT) Effect(op communication.Operation) {
	if r.after(op, r.Unstable_operations) {
		r.Unstable_st = r.Data.Apply(r.Unstable_st, mapset.NewSet(op))
		r.Unstable_operations.Add(op)
	} else {
		r.Unstable_operations.Add(op)
		r.Unstable_st = r.Data.Apply(r.Stable_st, r.Data.Order(r.Unstable_operations))
	}
}

func (r *EcroCRDT) Stabilize(op communication.Operation) {
	r.Unstable_operations.Remove(op)
	r.Data.Apply(r.Stable_st, mapset.NewSet(op))
}

func (r *EcroCRDT) Query() any {
	return r.Unstable_st
}

// checks if op commutes with all unstable concurrent operations
func (r *EcroCRDT) commutes(op communication.Operation, operations mapset.Set[communication.Operation]) bool {
	for _, op2 := range operations.ToSlice() {
		if !r.Data.Commutes(op, op2) {
			return false
		}
	}
	return true
}

func (r *EcroCRDT) after(op communication.Operation, operations mapset.Set[communication.Operation]) bool {
	// commutes and order_after only with concurrent operations
	if r.commutes(op, operations) || r.order_after(op, operations) || r.causally_after(op, operations) {
		return true
	}
	return false
}

func (r *EcroCRDT) order_after(op communication.Operation, operations mapset.Set[communication.Operation]) bool {
	operations.Add(op)
	r.Data.Order(operations)
	op1, _ := operations.Pop()
	return op1 == op
}

func (r *EcroCRDT) causally_after(op communication.Operation, operations mapset.Set[communication.Operation]) bool {
	for _, op2 := range operations.ToSlice() {
		if op.Version.Compare(*op2.Version) != communication.Ancestor {
			return false
		}
	}

	return true
}
