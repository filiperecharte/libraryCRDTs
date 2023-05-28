package crdt

import (
	"library/packages/communication"
)

type CommutativeStableDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	Stabilize(state any, op communication.Operation) any
}

type CommutativeStableCRDT struct {
	Data        CommutativeStableDataI
	Stable_st   any
	N_Ops       uint64
}

// effect
func (c *CommutativeStableCRDT) Effect(op communication.Operation) {
	c.Stable_st = c.Data.Apply(c.Stable_st, []communication.Operation{op})
	c.N_Ops++
}

func (c *CommutativeStableCRDT) Stabilize(op communication.Operation) {
	c.Stable_st = c.Data.Stabilize(c.Stable_st, op)
}

func (c *CommutativeStableCRDT) Query() any {
	return c.Stable_st
}

func (c *CommutativeStableCRDT) NumOps() uint64 {
	return c.N_Ops
}
