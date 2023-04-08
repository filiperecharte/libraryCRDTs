package crdts

import (
	"fmt"
	"library/packages/communication"
	"library/packages/replica"
)

type Counter struct {
	state int
}

func (r *Counter) TCDeliver(msg communication.Message) {
	r.state += msg.Value.(int)
}

func (r *Counter) TCStable(msg communication.Message) {
	fmt.Println("Ignoring received stable operation: ", msg)
}

func (r *Counter) Query() interface{} {
	return r.state
}

// initialize counter
func NewCounter(id string, channels map[string]chan interface{}, delay bool) *replica.Replica {
	c := &Counter{
		state: 0,
	}

	return replica.NewReplica(id, c, channels, delay)
}
