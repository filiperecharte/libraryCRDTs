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

func (a AddWins) Apply(state any, operations mapset.Set[any]) any {
	st := state.(int)
	for _, op := range operations.ToSlice() {
		msgOP := op.(communication.Message)
		st += msgOP.Value.(int)
	}
	return st
}

func (a AddWins) Order(operations mapset.Set[any]) mapset.Set[any] {
	return operations
}

func (a AddWins) Commutes(op1 any, op2 any) bool {
	return true
}

// initialize counter replica
func NewAddWinsReplica(id string, channels map[string]chan any) *replica.Replica {

	aw := AddWins{operations: map[string]interface{}{
		"ADD": func(a int, b int) int {
			return a + b
		},
		"REM": func(a int, b int) int {
			return a - b
		}}}

	c := crdt.EcroCRDT{Data: aw, Stable_st: 0}

	return replica.NewReplica(id, &c, channels)
}
