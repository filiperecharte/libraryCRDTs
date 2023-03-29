package replica

import (
	"fmt"
	"library/packages/communication"
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
	VersionVector communication.VClock
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
		VersionVector: communication.InitVClock(ids), //delivered version vector
	}

	go r.dequeue()

	return r
}

// Broadcasts a message by incrementing the replica's own entry in the version vector
// and enqueuing the message with the updated version vector to the middleware process.
func (r *Replica) TCBcast(op interface{}) {
	r.VersionVector[r.ID]++
	msg := communication.NewMessage(communication.DLV, op, r.VersionVector.Copy(), r.ID)
	r.TCDeliver(msg)
	r.Middleware.Tcbcast <- msg
}

// Dequeues a message that is ready to be delivered to the replica process.
// Increments the sender's entry in the replica's version vector before calling the TCDeliver callback.
func (r *Replica) dequeue() {
	for {
		msg := <-r.Middleware.DeliverCausal
		if msg.Type == communication.DLV {
			r.VersionVector[msg.OriginID] = msg.Version[msg.OriginID]
			r.TCDeliver(msg)
		} else if msg.Type == communication.STB {
			r.TCStable(msg)
		}
	}
}

// The TCDeliver callback function is called when a message is ready to be delivered.
func (r *Replica) TCDeliver(msg communication.Message) {
	r.Unstable = append(r.Unstable, msg)
}

// The TCStable callback function is called when a message is set to stable.
func (r *Replica) TCStable(msg communication.Message) {
	fmt.Println("Replica", r.ID, "received stable operation: ", msg)
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
