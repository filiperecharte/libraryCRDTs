package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type AddWins struct {
	operations map[string]interface{}
}

func (a AddWins) Apply(state any, operations mapset.Set[communication.Operation]) any {
	st := state.(int)
	for _, op := range operations.ToSlice() {
		msgOP := op
		st += msgOP.Value.(int)
	}
	return st
}

func (a AddWins) Order(operations mapset.Set[communication.Operation]) mapset.Set[communication.Operation] {
	return operations
}

func (a AddWins) Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	return true
}

// initialize counter replica
func NewAddWinsReplica(id string, channels map[string]chan any) *replica.Replica {

	aw := AddWins{operations: map[string]interface{}{
		"ADD": func(a mapset.Set[any], b any) mapset.Set[any] {
			a.Append(b)
			return a
		},
		"REM": func(a mapset.Set[any], b any) mapset.Set[any] {
			a.Remove(b)
			return a
		}}}

	c := crdt.EcroCRDT{Data: aw, Stable_st: 0}

	return replica.NewReplica(id, &c, channels)
}
