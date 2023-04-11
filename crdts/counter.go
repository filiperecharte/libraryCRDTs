package crdts

import (
	"library/packages/communication"
	"library/packages/replica"
	"log"
)

type Counter struct {
	state  int
	stable []communication.Message
}

func (r *Counter) TCDeliver(msg communication.Message) {
	r.state += msg.Value.(int)
}

func (r *Counter) TCStable(msg communication.Message) {
	r.stable = append(r.stable, msg)
	log.Println("STABLE", r.stable)
}

func (r *Counter) Query() interface{} {
	return r.state
}

// initialize counter
func NewCounter(id string, channels map[string]chan interface{}) *replica.Replica {
	c := &Counter{
		state: 0,
	}

	return replica.NewReplica(id, c, channels)
}
