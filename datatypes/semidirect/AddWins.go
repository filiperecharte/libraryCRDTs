package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type AddWins struct {
	id string
}

func (a AddWins) Add(state mapset.Set[any], elem any) mapset.Set[any] {
	state.Add(elem.(communication.Operation).Value)
	return state
}

func (a AddWins) Remove(state mapset.Set[any], elem any) mapset.Set[any] {
	state.Remove(elem.(communication.Operation).Value)
	return state
}

func (a AddWins) Apply(state any, operations []communication.Operation) any {
	st := state.(mapset.Set[any]).Clone()
	for _, op := range operations {
		switch op.Type {
		case "Add":
			state = a.Add(st, op)
		case "Rem":
			state = a.Remove(st, op)
		case "Nop": //has to commute with Rem
			continue
		}
	}
	return st
}

func (a AddWins) Repair(op1 communication.Operation, op2 communication.Operation) communication.Operation {
	//removes come before adds
	//we have to classes of updates: add and rem, and adds have priority over rems
	//the result of repair has to be an operation on the "lower" class, on this case commutative with rem
	//the result of a repair in an AddWins is Nop
	//we want rem -> add to always happen. rem -> add = add -> add |> rem, this equality is true if add |> rem = nop

	if op1.Type == "Add" && op2.Type == "Rem" && op1.Value == op2.Value {
		return communication.Operation{Type: "Nop", Value: nil, Version: op2.Version}
	}

	return op2
}

func (a AddWins) ArbitrationConstraint(op communication.Operation) bool {
	if op.Type == "Add" {
		return true
	}
	return false
}

// initialize counter replica
func NewAddWinsReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := crdt.SemidirectCRDT{Id: id, Data: AddWins{id}, Unstable_operations: []communication.Operation{}, Unstable_st: mapset.NewSet[any](), N_Ops: 0}

	return replica.NewReplica(id, &c, channels, delay)
}
