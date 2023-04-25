package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
	"sort"
)

type AddWins struct {
}

func (a AddWins) Add(state []communication.Operation, elem any) []communication.Operation {
	return append(state, elem.(communication.Operation))
}

func (a AddWins) Remove(state []communication.Operation, elem any) []communication.Operation {
	var result []communication.Operation

	for _, op := range state {
		if op.Value != elem.(communication.Operation).Value && op.Type != elem.(communication.Operation).Type {
			result = append(result, op)
		}
	}

	return result
}

func (a AddWins) Apply(state any, operations []communication.Operation) any {
	for _, op := range operations {
		switch op.Type {
		case "Add":
			state = a.Add(state.([]communication.Operation), op)
		case "Remove":
			state = a.Remove(state.([]communication.Operation), op)
		}
	}
	return state
}

func (a AddWins) Order(operations []communication.Operation) []communication.Operation {
	//order map of operations by type of operation, removes come before adds
	sort.Slice(operations, func(i, j int) bool {
		//if operations[i].Concurrent(&operations[j]) {
		if operations[i].Type == "Add" && operations[j].Type == "Remove" {
			return false
		}
		if operations[i].Type == "Remove" && operations[j].Type == "Add" {
			return true
		}
		//}
		return true
	})
	return operations
}

func (a AddWins) Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type != op2.Type
}

// initialize counter replica
func NewAddWinsReplica(id string, channels map[string]chan any) *replica.Replica {

	c := crdt.EcroCRDT{Data: AddWins{}, Stable_st: []communication.Operation{}, Unstable_operations: []communication.Operation{}, Unstable_st: []communication.Operation{}}

	return replica.NewReplica(id, &c, channels)
}
