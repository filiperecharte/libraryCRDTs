package crdt

import (
	"library/packages/communication"
	"log"
)

// all updates are reparable
type SemidirectDataI interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// Updates of the bigger class
	ArbitrationConstraint(op communication.Operation) bool

	// Repairs unstable operations.
	Repair(op1 communication.Operation, op2 communication.Operation) communication.Operation
}

type SemidirectCRDT struct {
	Id                  string
	Data                SemidirectDataI           //data interface
	Unstable_operations []communication.Operation //all aplied updates
	Unstable_st         any
	N_Ops               uint64
	S_Ops               uint64
}

func (r *SemidirectCRDT) Effect(op communication.Operation) {
	newOp := r.repair(op)
	r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{newOp})

	if r.Data.ArbitrationConstraint(newOp) {
		r.Unstable_operations = append(r.Unstable_operations, op)
	}
	r.N_Ops++
	log.Println(r.Id, r.N_Ops)
}

func (r *SemidirectCRDT) Stabilize(op communication.Operation) {
	for i, o := range r.Unstable_operations {
		if o.Equals(op) {
			r.Unstable_operations = append(r.Unstable_operations[:i], r.Unstable_operations[i+1:]...)
			break
		}
	}
	r.S_Ops++
}

func (r *SemidirectCRDT) RemovedEdge(op communication.Operation) {
	//ignore
}

func (r *SemidirectCRDT) Query() (any, any) {
	return r.Unstable_st, nil
}

func (r *SemidirectCRDT) NumOps() uint64 {
	return r.N_Ops
}

func (r *SemidirectCRDT) NumSOps() uint64 {
	return r.S_Ops
}

func (r *SemidirectCRDT) repair(op communication.Operation) communication.Operation {

	//find operations that is concurrent with op
	for _, o := range r.Unstable_operations {
		if o.Version.Compare(op.Version) == communication.Concurrent {
			op = r.Data.Repair(o, op)
		}
	}

	return op
}
