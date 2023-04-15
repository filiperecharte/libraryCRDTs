package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
)

type PNCounter struct{}

func (r PNCounter) Apply(state any, operations []any) any {
	st := state.(int)
	for _, op := range operations {
		msgOP := op.(communication.Message)
		switch msgOP.Type {
		case communication.ADD:
			st += msgOP.Value.(int)
		case communication.REM:
			st -= msgOP.Value.(int)
		}
	}
	return st
}

// initialize counter
func NewPNCounterReplica(id string, channels map[string]chan interface{}) *replica.Replica {

	c := crdt.CommutativeCRDT{Data: PNCounter{}, Stable_st: 0}

	return replica.NewReplica(id, &c, channels)
}
