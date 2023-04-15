package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
)

type Counter struct{}

func (c Counter) Apply(state any, operations []any) any {
	st := state.(int)
	for _, op := range operations {
		msgOP := op.(communication.Message)
		st += msgOP.Value.(int)
	}
	return st
}

// initialize counter replica
func NewCounterReplica(id string, channels map[string]chan any) *replica.Replica {

	c := crdt.CommutativeCRDT{Data: Counter{}, Stable_st: 0}

	return replica.NewReplica(id, &c, channels)
}
