package crdts

import (
	"fmt"
	"library/packages/communication"
	"library/packages/replica"
)

type PNCounter struct {
	state int
}

func (r *PNCounter) TCDeliver(msg communication.Message) {
	switch msg.Type {
	case communication.ADD:
		r.state += msg.Value.(int)
	case communication.REM:
		r.state -= msg.Value.(int)
	}
}

func (r *PNCounter) TCStable(msg communication.Message) {
	fmt.Println("Ignoring received stable operation: ", msg)
}

func (r *PNCounter) Query() interface{} {
	return r.state
}

// initialize counter
func NewPNCounter(id string, channels map[string]chan interface{}) *replica.Replica {
	c := &Counter{
		state: 0,
	}

	return replica.NewReplica(id, c, channels)
}
