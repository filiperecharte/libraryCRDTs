package replica

import (
	"fmt"
	"library/packages/crdt"
	"library/packages/middleware"
	"library/packages/utils"
)

//type ReplicaID string

type Replica struct {
	ID            string
	replicaIDS    []string
	Crdt          crdt.Crdt
	State         interface{}
	Unstable      []interface{} // Event operations waiting to stabilize
	Middleware    *middleware.Middleware
	VersionVector middleware.VClock
}

func NewReplica(id string, ids []string, crdt crdt.Crdt) *Replica {
	//initialize replica state

	r := &Replica{
		ID:            id,
		replicaIDS:    ids,
		Crdt:          crdt,
		State:         crdt.Default(),
		Middleware:    middleware.NewMiddleware(id, ids, len(ids)),
		VersionVector: middleware.InitVClock(ids, uint64(len(ids))), //delivered version vector
	}

	go r.dequeue()

	return r
}

// Broadcasts a message by incrementing the replica's own entry in the version vector
// and enqueuing the message with the updated version vector to the middleware process.
func (r *Replica) TCBcast(msg interface{}) {
	r.VersionVector[r.ID]++
	payload := *middleware.NewMessage(msg, r.VersionVector, r.ID)
	r.Middleware.Tcbcast <- payload
}

// Dequeues a message that is ready to be delivered to the replica process.
// Increments the sender's entry in the replica's version vector before calling the TCDeliver callback.
func (r *Replica) dequeue() {
	for {
		msg := <-r.Middleware.DeliverCausal
		r.VersionVector[msg.OriginID] = msg.Version[msg.OriginID]
		r.TCDeliver(msg)
	}
}

// The TCDeliver callback function is called when a message is ready to be delivered.
func (r *Replica) TCDeliver(msg middleware.Message) {
	fmt.Println("Delivered message to replica", r.ID)
	r.Unstable = append(r.Unstable, msg)
}

// Update made by a client to a replica that receives the operation to be applied to the CRDT
// sends the operation to middleware for broadcast
func (r *Replica) Update(op interface{}) {
	r.TCBcast(op)
}

// Query made by a client to a replica that returns the current state of the CRDT
// after applying the unstable operations into the CRDT stable state
func (r *Replica) Query() interface{} {
	return r.Crdt.Apply(r.State, utils.MessagesToValues(r.Unstable))
}
