package crdt

import (
	"library/packages/communication"

	mapset "github.com/deckarep/golang-set/v2"
)

// data interface
type EcroDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations mapset.Set[any]) any

	// Order unstable operations.
	Order(operations mapset.Set[any]) mapset.Set[any]

	//Operations that commute
	Commutes(op1 any, op2 any) bool
}

type EcroCRDT struct {
	Data                EcroDataI //data interface
	Stable_st           any       // stable state
	Unstable_operations mapset.Set[any]
	Unstable_st         any //most recent state
}

func (r *EcroCRDT) TCDeliver(msg communication.Message) {
	if r.after(msg.Value, r.Unstable_operations) {
		r.Unstable_st = r.Data.Apply(r.Unstable_st, mapset.NewSet(msg.Value))
		r.Unstable_operations.Add(msg.Value)
	} else {
		r.Unstable_operations.Add(msg.Value)
		r.Unstable_st = r.Data.Apply(r.Stable_st, r.Data.Order(r.Unstable_operations))
	}
}

func (r *EcroCRDT) TCStable(msg communication.Message) {
	r.Unstable_operations.Remove(msg.Value)
	r.Data.Apply(r.Stable_st, mapset.NewSet(msg.Value))
}

func (r *EcroCRDT) Query() any {
	return r.Unstable_st
}

// checks if op commutes with all unstable concurrent operations
func (r *EcroCRDT) commutes(op any, operations mapset.Set[any]) bool {
	for _, op2 := range operations.ToSlice() {
		if !r.Data.Commutes(op, op2) {
			return false
		}
	}
	return true
}

func (r *EcroCRDT) after(op any, operations mapset.Set[any]) bool {
	// commutes and order_after only with concurrent operations
	if r.commutes(op, operations) || r.order_after(op, operations) || r.causally_after(op, operations) {
		return true
	}
	return false
}

func (r *EcroCRDT) order_after(op any, operations mapset.Set[any]) bool {
	operations.Add(op)
	r.Data.Order(operations)
	op1, _ := operations.Pop()
	return op1 == op
}

func (r *EcroCRDT) causally_after(op any, operations mapset.Set[any]) bool {
	for _, op2 := range operations.ToSlice() {
		if op.(communication.Message).Version.Compare(op2.(communication.Message).Version) != communication.Ancestor {
			return false
		}
	}

	return true
}
