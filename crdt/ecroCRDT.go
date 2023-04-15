package crdt

import (
	"library/packages/communication"

	mapset "github.com/deckarep/golang-set/v2"
)

//data interface
type EcroDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations mapset.Set[any]) any

	// Order unstable operations.
	Order(operations mapset.Set[any]) mapset.Set[any]

	//Operations that commute
	Commutes(op1 any, operations mapset.Set[any]) bool
}

type EcroCRDT struct {
	data EcroDataI //data interface
	stable_st           any
	unstable_operations mapset.Set[any]
	unstable_st         any
}

func (r *EcroCRDT) TCDeliver(msg communication.Message) {
	if r.after(msg.Value, r.unstable_operations) {
		r.unstable_st = r.data.Apply(r.unstable_st, mapset.NewSet(msg.Value))
		r.unstable_operations.Add(msg.Value)
	} else {
		r.unstable_operations.Add(msg.Value)
		r.unstable_st = r.data.Apply(r.stable_st, r.data.Order(r.unstable_operations))
	}
}

func (r *EcroCRDT) TCStable(msg communication.Message) {
	r.data.Apply(r.stable_st, mapset.NewSet(msg.Value))
}

func (r *EcroCRDT) Query() any {
	return r.unstable_st
}

func (r *EcroCRDT) after(op any, operations mapset.Set[any]) bool {
	// commutes and order_after only with concurrent operations
	if r.data.Commutes(op, operations) || r.order_after(op, operations) || r.causally_after(op, operations) {
		return true
	}
	return false
}

func (r *EcroCRDT) order_after(op any, operations mapset.Set[any]) bool {
	operations.Add(op)
	r.data.Order(operations)
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
