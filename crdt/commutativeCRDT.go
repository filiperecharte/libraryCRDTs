package crdt

import (
	"library/packages/communication"
	"log"
)

type CommutativeDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any
}

type CommutativeCRDT struct {
	Data      CommutativeDataI
	Stable_st any
	N_Ops     uint64
	S_Ops     uint64
}

// effect
func (c *CommutativeCRDT) Effect(op communication.Operation) {
	c.Stable_st = c.Data.Apply(c.Stable_st, []communication.Operation{op})
	c.N_Ops++
	log.Println(c.N_Ops)
}

func (c *CommutativeCRDT) Stabilize(op communication.Operation) {
	//ignore
}

func (c *CommutativeCRDT) RemovedEdge(op communication.Operation) {
	//ignore
}

func (c *CommutativeCRDT) Query() (any, any) {
	return c.Stable_st, nil
}

func (c *CommutativeCRDT) NumOps() uint64 {
	return c.N_Ops
}

func (c *CommutativeCRDT) NumSOps() uint64 {
	return c.S_Ops
}
