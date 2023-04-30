package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
)

type Counter struct{}

func (c Counter) Apply(state any, operations []communication.Operation) any {
	st := state.(int)
	for _, op := range operations {
		st += op.Value.(int)
	}
	return st
}

// initialize counter replica
func NewCounterReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := crdt.CommutativeCRDT{Data: Counter{}, Stable_st: 0}

	return replica.NewReplica(id, &c, channels, delay)
}
