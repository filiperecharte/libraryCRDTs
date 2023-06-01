package crdt

import (
	"library/packages/communication"
)

// all updates are reparable
type Semidirect2DataI interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// ArbitrationOrder returns two booleans
	// the first tells if the op2 is repairable knowing op1
	// the second tells if the order op1 > op2 is correct or needs to be swapped
	ArbitrationOrder(op1 communication.Operation, op2 communication.Operation) (bool, bool)

	// Repairs unstable operations.
	Repair(op1 communication.Operation, op2 communication.Operation) communication.Operation
}

type Semidirect2CRDT struct {
	Id                  string
	Data                Semidirect2DataI          //data interface
	Unstable_operations []communication.Operation //all aplied updates
	Unstable_st         any
	N_Ops               uint64
}

func (r *Semidirect2CRDT) Effect(op communication.Operation) {

	for i, o := range r.Unstable_operations {
		repair, order := r.Data.ArbitrationOrder(o, op)
		if repair {
			op = r.Data.Repair(o, op)
		}
		if !order {
			//add op to Unstable_operations at position i
			r.Unstable_operations = append(r.Unstable_operations[:i], append([]communication.Operation{op}, r.Unstable_operations[i:]...)...)
		}
	}

	r.Unstable_st = r.Data.Apply(r.Unstable_st, r.Unstable_operations)
	r.N_Ops++
}

func (r *Semidirect2CRDT) Stabilize(op communication.Operation) {
	for i, o := range r.Unstable_operations {
		if o.Equals(op) {
			r.Unstable_operations = append(r.Unstable_operations[:i], r.Unstable_operations[i+1:]...)
			break
		}
	}
}

func (r *Semidirect2CRDT) Query() any {
	return r.Unstable_st
}

func (r *Semidirect2CRDT) NumOps() uint64 {
	return r.N_Ops
}
