package crdt

import (
	"library/packages/communication"
	"library/packages/replica"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
)

type AddWins struct {
	id            string
	state         map[any]communication.VClock
	N_Ops         uint64
	S_Ops         uint64
	StabilizeLock *sync.RWMutex
}

// effect
func (c *AddWins) Effect(op communication.Operation) {
	c.StabilizeLock.Lock()
	defer c.StabilizeLock.Unlock()

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
	c.StabilizeLock.Lock()
	defer c.StabilizeLock.Unlock()
	for i, v := range c.state {
		if i == op.Value && v.Equal(op.Version) {
			//removes the timestamp
			c.state[i] = communication.VClock{}
		}
	}
	c.S_Ops++
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

func (c *AddWins) NumSOps() uint64 {
	return c.S_Ops
}

// initialize counter replica
func NewAddWinsBaseReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	c := AddWins{id, map[any]communication.VClock{}, 0, 0, new(sync.RWMutex)}
	return replica.NewReplica(id, &c, channels, delay)
}

func (c *AddWins) PrintOpsEffect() {
	c.N_Ops++
	if c.N_Ops%1000 == 0 {
		println("effect", c.N_Ops)
	}
}

func (c *AddWins) PrintOpsStabilize() {
	c.S_Ops++
}
