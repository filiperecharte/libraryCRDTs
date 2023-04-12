package crdts

import (
	"library/packages/communication"
	"library/packages/replica"
)

type Counter struct {
	state []int
}

func (r *Counter) TCDeliver(msg communication.Message) {
	r.state = append(r.state, msg.Value.(int))
}

func (r *Counter) TCStable(msg communication.Message) {
	//ignore
}

func (r *Counter) Query() interface{} {
	return r.state
}

// initialize counter
func NewCounter(id string, channels map[string]chan interface{}) *replica.Replica {
	c := &Counter{
		state: []int{},
	}

	return replica.NewReplica(id, c, channels)
}
