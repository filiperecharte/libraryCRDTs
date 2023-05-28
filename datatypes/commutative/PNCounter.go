package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
)

type PNCounter struct{}

func (r PNCounter) Apply(state any, operations []communication.Operation) any {
	st := state.(int)
	for _, op := range operations {
		msgOP := op
		switch msgOP.Type {
		case "Add":
			st += msgOP.Value.(int)
		case "Rem":
			st -= msgOP.Value.(int)
		}
	}
	return st
}

// initialize counter
func NewPNCounterReplica(id string, channels map[string]chan interface{}, delay int) *replica.Replica {

	c := crdt.CommutativeCRDT{Data: PNCounter{}, Stable_st: 0}

	return replica.NewReplica(id, &c, channels, delay)
}
