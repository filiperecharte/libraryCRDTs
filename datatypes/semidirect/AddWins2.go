package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
	"strconv"

	mapset "github.com/deckarep/golang-set/v2"
)

type RemValue struct {
	Value any
	T     mapset.Set[int]
}

type AddValue struct {
	Value any
	t     int
}

type AddWins2 struct {
	id string
}

func (a *AddWins2) Add(state mapset.Set[AddValue], op communication.Operation) mapset.Set[AddValue] {
	id, _ := strconv.Atoi(strconv.Itoa(int(op.Version.Sum())) + op.OriginID)
	state.Add(AddValue{op.Value, id})
	return state
}

func (a AddWins2) Remove(state mapset.Set[AddValue], op communication.Operation) mapset.Set[AddValue] {
	opValue, repaired := op.Value.(RemValue)
	if !repaired {
		opValue = RemValue{op.Value, mapset.NewSet[int]()}
	}

	for _, v := range state.ToSlice() {

		if v.Value == opValue.Value && !opValue.T.Contains(v.t) {
			//remove element from S
			state.Remove(AddValue{v.Value, v.t})
		}

	}

	return state
}

func (a AddWins2) Apply(state any, operations []communication.Operation) any {
	st := state.(mapset.Set[AddValue]).Clone()
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

func (a AddWins2) Repair(op1 communication.Operation, op2 communication.Operation) communication.Operation {
	if op1.Type == "Add" && op2.Type == "Rem" {

		remValue, repaired := op2.Value.(RemValue)
		if !repaired {
			remValue = RemValue{op2.Value, mapset.NewSet[int]()}
		}

		if op1.Value == remValue.Value {

			id1, _ := strconv.Atoi(strconv.Itoa(int(op1.Version.Sum())) + op1.OriginID)
			remValue.T.Add(id1) //adds add timestamp to T of remove
			return communication.Operation{Type: "Rem", Value: remValue, Version: op2.Version}

		}
	}

	return op2
}

func (a AddWins2) ArbitrationConstraint(op communication.Operation) bool {
	return op.Type == "Add"
}

// initialize counter replica
func NewAddWins2Replica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := crdt.SemidirectCRDT{Id: id, Data: AddWins2{id}, Unstable_operations: []communication.Operation{}, Unstable_st: mapset.NewSet[AddValue](), N_Ops: 0}

	return replica.NewReplica(id, &c, channels, delay)
}
