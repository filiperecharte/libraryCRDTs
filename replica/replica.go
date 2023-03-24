package replica

import "library/packages/middleware"

//type ReplicaID string

type Replica struct {
	ID         string
	State      interface{}
	Unstable   map[interface{}]struct{} // Event operations waiting to stabilize
	middleware *middleware.Middleware
}

func NewReplica(id string) *Replica {
	//initialize replica state

	return &Replica{
		ID:         id,
		State:      nil,
		Unstable:   make(map[interface{}]struct{}),
		middleware: middleware.NewMiddleware(),
	}
}

// Update made by a client to a replica that receives the operation to be applied to the CRDT
// sends the operation to middleware and returns the new state of the CRDT
func (r *Replica) Update(op interface{}) interface{} {
	//update replica state

	// adds operation to unstable operations by receiving it from middleware or adding it itself (understand effect Pure-op based)
	// TODO

	return middleware.Update(r.ID, r.middleware, op)
}

// Query made by a client to a replica that returns the current state of the CRDT
// after appliying the unstable operations into the CRDT stable state
func (r *Replica) Query() interface{} {
	//query replica state

	//apply unstable operations to stable state
	// TODO

	return r.State
}
