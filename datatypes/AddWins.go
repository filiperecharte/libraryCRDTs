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
		}
	}
	return st
}

func (a AddWins) Order(op1 communication.Operation, op2 communication.Operation) bool {
	//order map of operations by type of operation, removes come before adds

	return op1.Type == "Rem" && op2.Type == "Add"
}

func (a AddWins) Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == op2.Type
}

func (a AddWins) Equals(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Value == op2.Value
}

// initialize counter replica
func NewAddWinsReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := crdt.EcroCRDT{Id: id, Data: AddWins{id}, Stable_st: mapset.NewSet[any](), Unstable_operations: []communication.Operation{}, Unstable_st: mapset.NewSet[any](), N_Ops: 0}

	return replica.NewReplica(id, &c, channels, delay)
}
