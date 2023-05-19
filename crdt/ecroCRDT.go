package crdt

import (
	"library/packages/communication"
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
}

func (r *EcroCRDT) Effect(op communication.Operation) {
	if r.after(op, r.Unstable_operations) {
		r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{op})
		r.Unstable_operations = append(r.Unstable_operations, op)
	} else {
		r.Unstable_operations = append(r.Unstable_operations, op)
		r.Unstable_st = r.Data.Apply(r.Stable_st, r.order(r.Unstable_operations))
	}
	r.N_Ops++
}

func (r *EcroCRDT) Stabilize(op communication.Operation) {
	//add op to stable slice
	r.Stable_operations = append(r.Stable_operations, op)
	orderedOperations := r.order(r.Unstable_operations)

	//remove op from slice
	first := orderedOperations[0]
	//check if first exists in stable slice
	for i, o := range r.Stable_operations {
		if first.Equals(o) {
			r.Stable_operations = append(r.Stable_operations[:i], r.Stable_operations[i+1:]...)

			//remove first from unstable slice
			for i1, o1 := range r.Unstable_operations {
				if first.Equals(o1) {
					r.Unstable_operations = append(r.Unstable_operations[:i1], r.Unstable_operations[i1+1:]...)
					break
				}
			}

			r.Stable_st = r.Data.Apply(r.Stable_st, []communication.Operation{op})
			r.Unstable_st = r.Data.Apply(r.Stable_st, r.order(r.Unstable_operations))
			break
		}
	}
}

func (r *EcroCRDT) Query() any {
	// log.Println("REPLICA", r.Id, "UNORDERED", r.Unstable_operations)
	// sortedOperations := r.order(r.Unstable_operations)
	// log.Println("REPLICA", r.Id, "ORDERED")
	// for _, op := range sortedOperations {
	// 	log.Println(op)
	// }
	return r.Unstable_st
}

func (r *EcroCRDT) NumOps() uint64 {
	return r.N_Ops
}

func (r *EcroCRDT) after(op communication.Operation, operations []communication.Operation) bool {
	// commutes and order_after only with concurrent operations
	if r.commutes(op, operations) || r.causally_after(op, operations) || r.order_after(op, operations) {
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

// it is safe to apply an update after all unstable operations when, if ordered, it would be the last operation
func (r *EcroCRDT) order_after(op communication.Operation, operations []communication.Operation) bool {
	for _, op2 := range operations {
		if op.Concurrent(op2) && !r.Data.Order(op2, op) && !r.Data.Commutes(op2, op) {
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
			if sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Ancestor || sortedOperations[i].OriginID > sortedOperations[j].OriginID {
				sortedOperations[i], sortedOperations[j] = sortedOperations[j], sortedOperations[i]
			}
		}
	}

	for i := 1; i < len(sortedOperations); i++ {
		for j := i - 1; j >= 0; j-- {

			if j == 0 {
				if i == j+1 {
					break
				}

				op := sortedOperations[i]
				sortedOperations = append(sortedOperations[:i], sortedOperations[i+1:]...)
				sortedOperations = append([]communication.Operation{sortedOperations[0]}, append([]communication.Operation{op}, sortedOperations[1:]...)...)

				break
			}

			if sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Ancestor || (sortedOperations[i].Concurrent(sortedOperations[j]) && r.Data.Order(sortedOperations[j], sortedOperations[i]) && !r.Data.Commutes(sortedOperations[j], sortedOperations[i])) {
				if i == j+1 {
					break
				}

				op1 := sortedOperations[i]
				sortedOperations = append(sortedOperations[:i], sortedOperations[i+1:]...)
				sortedOperations = append(sortedOperations[:j+1], append([]communication.Operation{op1}, sortedOperations[j+1:]...)...)

				break
			}
		}
	}

	return sortedOperations
}
