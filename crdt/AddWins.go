package crdt

import (
	"library/packages/communication"
	"library/packages/replica"

	mapset "github.com/deckarep/golang-set/v2"
)

type AddWins struct {
	state map[any]communication.VClock
	N_Ops uint64
}

// effect
func (c *AddWins) Effect(op communication.Operation) {
	switch op.Type {
	case "Add":
		c.state[op.Value] = op.Version
	case "Rem":
		for i, v := range c.state {
			if i == op.Value && v.Compare(op.Version) == communication.Ancestor {
				//removes the element from the slice
				delete(c.state, i)
			}
		}
	}

	c.N_Ops++
}

func (c *AddWins) Stabilize(op communication.Operation) {
	for i, v := range c.state {
		if i == op.Value && v.Equal(op.Version) {
			//removes the timestamp
			c.state[i] = communication.VClock{}
		}
	}
}

func (c *AddWins) Query() (any, any) {
	set := mapset.NewSet[any]()
	for i, _ := range c.state {
		set.Add(i)
	}
	return set, nil
}

func (c *AddWins) NumOps() uint64 {
	return c.N_Ops
}

// initialize counter replica
func NewAddWinsBaseReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := AddWins{map[any]communication.VClock{}, 0}
	return replica.NewReplica(id, &c, channels, delay)
}
