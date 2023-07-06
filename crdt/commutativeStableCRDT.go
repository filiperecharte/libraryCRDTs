package crdt

import (
	"library/packages/communication"
	"log"
)

type CommutativeStableDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	Stabilize(state any, op communication.Operation) any

	// Query returns the current state of the CRDT
	Query(state any) any
}

type CommutativeStableCRDT struct {
	Data      CommutativeStableDataI
	Stable_st any
	N_Ops     uint64
	S_Ops     uint64
}

// effect
func (c *CommutativeStableCRDT) Effect(op communication.Operation) {
	c.Stable_st = c.Data.Apply(c.Stable_st, []communication.Operation{op})
	c.N_Ops++
	log.Println(c.N_Ops)
}

func (c *CommutativeStableCRDT) Stabilize(op communication.Operation) {
	c.Stable_st = c.Data.Stabilize(c.Stable_st, op)
	c.S_Ops++
}

func (c *CommutativeStableCRDT) RemovedEdge(op communication.Operation) {
	//ignore
}

func (c *CommutativeStableCRDT) Query() (any, any) {
	return c.Data.Query(c.Stable_st), nil
}

func (c *CommutativeStableCRDT) NumOps() uint64 {
	return c.N_Ops
}

func (c *CommutativeStableCRDT) NumSOps() uint64 {
	return c.S_Ops
}
