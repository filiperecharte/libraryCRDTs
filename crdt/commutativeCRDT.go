package crdt

import (
	"library/packages/communication"
)

type CommutativeDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any
}

type CommutativeCRDT struct {
	Data      CommutativeDataI
	Stable_st any
}

// effect
func (c *CommutativeCRDT) Effect(op communication.Operation) {
	c.Stable_st = c.Data.Apply(c.Stable_st, []communication.Operation{op})
}

func (c *CommutativeCRDT) Stabilize(op communication.Operation) {
	//ignore
}

func (c *CommutativeCRDT) Query() any {
	return c.Stable_st
}
