package crdt

import (
	"library/packages/communication"
	"sync"
)

// data interface
type EcroDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// Order unstable operations.
	Order(op1 communication.Operation, op2 communication.Operation) bool

	//Operations that commute
	Commutes(op1 communication.Operation, op2 communication.Operation) bool
}

type EcroCRDT struct {
	Id                  string
	Data                EcroDataI //data interface
	Stable_st           any       // stable state
	Unstable_operations []communication.Operation
	Stable_operations   []communication.Operation
	Unstable_st         any //most recent state
	N_Ops               uint64

	StabilizeLock *sync.RWMutex
}



func (r *EcroCRDT) Effect(op communication.Operation) {
	r.StabilizeLock.Lock()
	if r.after(op, r.Unstable_operations) {
		r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{op})
		r.Unstable_operations = append(r.Unstable_operations, op)
	} else {
		r.Unstable_operations = append(r.Unstable_operations, op)
		r.Unstable_st = r.Data.Apply(r.Stable_st, r.order(r.Unstable_operations))
	}
	r.StabilizeLock.Unlock()
	r.N_Ops++
}

func (r *EcroCRDT) Stabilize(op communication.Operation) {
	r.Stable_operations = append(r.Stable_operations, op)
	orderedOperations := r.order(r.Unstable_operations)

	first := orderedOperations[0]

	for i, o := range r.Stable_operations {
		if first.Equals(o) {
			r.Stable_operations = append(r.Stable_operations[:i], r.Stable_operations[i+1:]...)
			r.StabilizeLock.Lock()
			for i1, o1 := range r.Unstable_operations {
				if first.Equals(o1) {
					r.Unstable_operations = append(r.Unstable_operations[:i1], r.Unstable_operations[i1+1:]...)
					break
				}
			}
			r.Stable_st = r.Data.Apply(r.Stable_st, []communication.Operation{o})
			r.Unstable_st = r.Data.Apply(r.Stable_st, orderedOperations[1:])
			r.StabilizeLock.Unlock()
			break
		}
	}

}

func (r *EcroCRDT) Query() any {
	// log.Println("REPLICA", r.Id, "UNORDERED", r.Unstable_operations)
	// sortedOperations := r.order(r.Unstable_operations)
	// log.Println("REPLICA", r.Id, "ORDERED", sortedOperations)
	return r.Unstable_st
}

func (r *EcroCRDT) NumOps() uint64 {
	return r.N_Ops
}

func (r *EcroCRDT) after(op communication.Operation, operations []communication.Operation) bool {
	// commutes and order_after only with concurrent operations
	if r.commutes(op, operations) || r.causally_after(op, operations) {
		return true
	}
	return false
}

// it is safe to apply an update after all unstable operations when it commutes with all of the concurrent operations
func (r *EcroCRDT) commutes(op communication.Operation, operations []communication.Operation) bool {
	for _, op2 := range operations {
		if op.Concurrent(op2) && !r.Data.Commutes(op, op2) {
			return false
		}
	}
	return true
}

// it is safe to apply an update after all unstable operations when it is causally after all unstable operations
func (r *EcroCRDT) causally_after(op communication.Operation, operations []communication.Operation) bool {
	for _, op2 := range operations {
		if op.Version.Compare(op2.Version) != communication.Ancestor {
			return false
		}
	}

	return true
}

// order operations
func (r *EcroCRDT) order(operations []communication.Operation) []communication.Operation {
	sortedOperations := make([]communication.Operation, len(operations))
	copy(sortedOperations, operations)

	for i := 0; i < len(sortedOperations); i++ {
		for j := i + 1; j < len(sortedOperations); j++ {
			if sortedOperations[i].OriginID > sortedOperations[j].OriginID {
				sortedOperations[i], sortedOperations[j] = sortedOperations[j], sortedOperations[i]
			} else if sortedOperations[i].OriginID == sortedOperations[j].OriginID && sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Ancestor {
				sortedOperations[i], sortedOperations[j] = sortedOperations[j], sortedOperations[i]
			}
		}
	}

	for i := len(sortedOperations) - 2; i >= 0; i-- {
		for j := i + 1; j < len(sortedOperations); j++ {

			if sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Descendant || (sortedOperations[i].Concurrent(sortedOperations[j]) && r.Data.Order(sortedOperations[j], sortedOperations[i]) && !r.Data.Commutes(sortedOperations[j], sortedOperations[i])) {
				if i+1 == j {
					break
				}
				op1 := sortedOperations[i]
				sortedOperations = append(sortedOperations[:i], sortedOperations[i+1:]...)
				sortedOperations = append(sortedOperations[:j-1], append([]communication.Operation{op1}, sortedOperations[j-1:]...)...)
				break
			}

			if j == len(sortedOperations)-1 {
				if i+1 == j {
					break
				}
				op1 := sortedOperations[i]
				sortedOperations = append(sortedOperations[:i], sortedOperations[i+1:]...)
				sortedOperations = append(sortedOperations, op1)
				break
			}
		}
	}

	return sortedOperations
}
