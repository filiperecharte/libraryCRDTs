package crdt

import (
	"library/packages/communication"
	"log"
	"sync"
)

// all updates are reparable
type Semidirect2DataI interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// ArbitrationOrder returns two booleans
	// the first tells if the op2 is repairable knowing op1
	// the second tells if the order op1 > op2 is correct or needs to be swapped
	ArbitrationOrder(op1 communication.Operation, op2 communication.Operation, state any) (bool, bool)

	MainOp() string

	// Repairs unstable operations.
	Repair(op1 communication.Operation, op2 communication.Operation, state any) communication.Operation

	// Repairs unstable operations.
	RepairCausal(op1 communication.Operation, op2 communication.Operation) communication.Operation
}

type NonMainOp struct {
	Op              communication.Operation
	HigherTimestamp []communication.Operation //to stabilize Op, all operations with lower timestamp must be stable when Op is applied to the state
}

type Semidirect2CRDT struct {
	Id                   string
	Data                 Semidirect2DataI          //data interface
	Unstable_operations  []communication.Operation //all aplied updates
	StableMain_operation communication.Operation
	NonMain_operations   []NonMainOp
	Unstable_st          any
	N_Ops                uint64
	S_Ops                uint64

	effectLock *sync.RWMutex
}

// initialize semidirectcrdt
func NewSemidirect2CRDT(id string, state any, data Semidirect2DataI) *Semidirect2CRDT {
	c := Semidirect2CRDT{
		Id:                  id,
		Data:                data,
		Unstable_operations: []communication.Operation{},
		NonMain_operations:  []NonMainOp{},
		Unstable_st:         state,
		N_Ops:               0,
		S_Ops:               0,
		effectLock:          new(sync.RWMutex),
	}

	return &c
}

func (r *Semidirect2CRDT) Effect(op communication.Operation) {
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	r.N_Ops++
	log.Println(r.Id, r.N_Ops)

	if r.Data.MainOp() != op.Type {
		r.NonMain_operations = append(r.NonMain_operations, NonMainOp{op, []communication.Operation{}})
		return
	}

	op = r.repairCausal(op)

	newOp := r.repair(op)
	r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{newOp})

	//add operation to unstable operations
	//iterate starting from the end over unstable operations to find the correct position to insert the new operation
	if len(r.Unstable_operations) == 0 {
		r.Unstable_operations = append(r.Unstable_operations, op)
	} else {
		inserted := false
		for i := len(r.Unstable_operations) - 1; i >= 0; i-- {
			//if it respects arbitration order, insert it
			if _, ok := r.Data.ArbitrationOrder(r.Unstable_operations[i], op, r.Unstable_st); ok {
				r.Unstable_operations = append(r.Unstable_operations[:i+1], append([]communication.Operation{op}, r.Unstable_operations[i+1:]...)...)
				inserted = true
				break
			}
		}
		if !inserted {
			r.Unstable_operations = append([]communication.Operation{op}, r.Unstable_operations...)
		}
	}
}

func (r *Semidirect2CRDT) Stabilize(op communication.Operation) {
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	r.S_Ops++

	if r.Data.MainOp() == op.Type {
		r.StableMain_operation = op
	}

	for i, v := range r.NonMain_operations {
		//log.Println("COMPARING", v.HigherTimestamp, op.Version)
		if r.becameStable(v.HigherTimestamp) {
			r.NonMain_operations = append(r.NonMain_operations[:i], r.NonMain_operations[i+1:]...)
			break
		}
	}

	if r.Data.MainOp() != op.Type {
		//remove from non main operations
		for i, v := range r.NonMain_operations {
			if v.Op.Equals(op) {
				//r.NonMain_operations = append(r.NonMain_operations[:i], r.NonMain_operations[i+1:]...)
				r.NonMain_operations[i].HigherTimestamp = r.getGreatestOps()
				r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{op})
				break
			}
		}
		return
	}

	if r.Data.MainOp() != op.Type {
		return
	}

	io := r.indexOf(op)

	if !r.prefixStable(io) {
		return
	}

	//remove operation from unstable operations
	r.Unstable_operations = append(r.Unstable_operations[:io], r.Unstable_operations[io+1:]...)
}

func (r *Semidirect2CRDT) RemovedEdge(op communication.Operation) {
	//ignore
}

func (r *Semidirect2CRDT) Query() (any, any) {
	//apply all non main operations
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	nonMainOp := r.getNonMainOperations()
	query_st := r.Data.Apply(r.Unstable_st, nonMainOp)
	return query_st, nonMainOp
}

func (r *Semidirect2CRDT) NumOps() uint64 {
	return r.N_Ops
}

func (r *Semidirect2CRDT) NumSOps() uint64 {
	return r.S_Ops
}

func (r *Semidirect2CRDT) repair(op communication.Operation) communication.Operation {
	//find operations that is concurrent with op

	for _, o := range r.Unstable_operations {
		if o.Version.Compare(op.Version) == communication.Concurrent {
			op = r.Data.Repair(o, op, r.Unstable_st)
		}
	}

	return op
}

func (r *Semidirect2CRDT) repairCausal(op communication.Operation) communication.Operation {
	for _, nonOP := range r.NonMain_operations {
		if nonOP.Op.Version.Compare(op.Version) == communication.Descendant {
			op = r.Data.RepairCausal(nonOP.Op, op)
		}
	}

	return op
}

// check if prefix of the operations is stable (all operations of the prefix are in stable_operations)
func (r Semidirect2CRDT) prefixStable(index int) bool {
	for _, o := range r.Unstable_operations[:index+1] {
		if r.StableMain_operation.Version.Compare(o.Version) != communication.Descendant {
			return false
		}
	}
	return true
}

// gets index of operation in array
func (r Semidirect2CRDT) indexOf(op communication.Operation) int {
	for i, o := range r.Unstable_operations {
		if op.Equals(o) {
			return i
		}
	}
	return -1
}

func (r Semidirect2CRDT) getNonMainOperations() []communication.Operation {
	nonMainOps := []communication.Operation{}
	for _, op := range r.NonMain_operations {
		nonMainOps = append(nonMainOps, op.Op)
	}
	return nonMainOps
}

func (r Semidirect2CRDT) getGreatestOps() []communication.Operation {

	if len(r.Unstable_operations) == 0 {
		return []communication.Operation{}
	}

	//get greatest operations
	greatestOps := []communication.Operation{}
	greatestOp := r.Unstable_operations[len(r.Unstable_operations)-1]
	greatestOps = append(greatestOps, greatestOp)

	for i := len(r.Unstable_operations) - 2; i >= 0; i-- {
		//if its concurrent, add it to the list
		if r.Unstable_operations[i].Version.Compare(greatestOp.Version) == communication.Concurrent {
			greatestOps = append(greatestOps, r.Unstable_operations[i])
		}
	}

	return greatestOps
}

func (r Semidirect2CRDT) becameStable(ops []communication.Operation) bool {
	if len(ops) == 0 {
		return false
	}

	for _, op := range ops {
		if op.Version.Compare(r.StableMain_operation.Version) != communication.Descendant {
			return false
		}
	}
	return true
}

func (r *Semidirect2CRDT) PrintOpsEffect() {
	r.N_Ops++
	if r.N_Ops%1000 == 0 {
		println("effect", r.N_Ops)
	}
}

func (r *Semidirect2CRDT) PrintOpsStabilize() {
	r.S_Ops++
	println("stabilize", r.S_Ops)
}
