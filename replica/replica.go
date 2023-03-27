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
	replicas      map[string]chan interface{}
	Crdt          crdt.Crdt
	State         interface{}
	Unstable      []interface{} // operations waiting to stabilize
	Middleware    *middleware.Middleware
	VersionVector middleware.VClock
}

func NewReplica(id string, crdt crdt.Crdt, channels map[string]chan interface{}, delay bool) *Replica {
	//initialize replica state

	ids := utils.MapToKeys(channels)

	r := &Replica{
		ID:            id,
		replicas:      channels,
		Crdt:          crdt,
		State:         crdt.Default(),
		Middleware:    middleware.NewMiddleware(id, ids, channels, delay),
		VersionVector: middleware.InitVClock(ids, uint64(len(ids))), //delivered version vector
	}

	go r.dequeue()

	return r
}

// Broadcasts a message by incrementing the replica's own entry in the version vector
// and enqueuing the message with the updated version vector to the middleware process.
func (r *Replica) TCBcast(op interface{}) {
	r.VersionVector[r.ID]++
	msg := middleware.NewMessage(middleware.DLV, op, r.VersionVector.Copy(), r.ID)
	r.TCDeliver(msg)
	r.Middleware.Tcbcast <- msg
}

// Dequeues a message that is ready to be delivered to the replica process.
// Increments the sender's entry in the replica's version vector before calling the TCDeliver callback.
func (r *Replica) dequeue() {
	for {
		msg := <-r.Middleware.DeliverCausal
		if msg.Type == middleware.DLV {
			r.VersionVector[msg.OriginID] = msg.Version[msg.OriginID]
			r.TCDeliver(msg)
		} else if msg.Type == middleware.STB {
			r.TCDeliver(msg)
		}
	}
}

// The TCDeliver callback function is called when a message is ready to be delivered.
func (r *Replica) TCDeliver(msg middleware.Message) {
	r.Unstable = append(r.Unstable, msg.Value)
}

// The TCStable callback function is called when a message is set to stable.
func (r *Replica) TCStable(msg middleware.Message) {
	/* do things */
}

// Update made by a client to a replica that receives the operation to be applied to the CRDT
// sends the operation to middleware for broadcast
func (r *Replica) Update(op interface{}) {
	r.TCBcast(op)
}

// Query made by a client to a replica that returns the current state of the CRDT
// after applying the unstable operations into the CRDT stable state
func (r *Replica) Query() interface{} {
	fmt.Println("Querying replica", r.ID, "with unstable operations", r.Unstable)
	return r.Crdt.Apply(r.State, r.Unstable)
}
