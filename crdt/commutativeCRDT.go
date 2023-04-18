package crdt

import (
	"library/packages/communication"
)

type CommutativeDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []any) any
}

type CommutativeCRDT struct {
	Data      CommutativeDataI
	Stable_st any
}

// effect
func (c *CommutativeCRDT) Effect(msg communication.Message) {
	c.Stable_st = c.Data.Apply(c.Stable_st, []any{msg})
}

func (c *CommutativeCRDT) Stabilize(msg communication.Message) {
	//ignore
}

func (c *CommutativeCRDT) Query() any {
	return c.Stable_st
}
