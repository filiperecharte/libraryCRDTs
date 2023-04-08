package replica

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/middleware"
	"library/packages/utils"
)

//type ReplicaID string

type Replica struct {
	id       string
	replicas map[string]chan interface{}
	crdt.Crdt
	middleware    *middleware.Middleware
	VersionVector communication.VClock
}

func NewReplica(id string, crdt crdt.Crdt, channels map[string]chan interface{}, delay bool) *Replica {
	//initialize replica state

	ids := utils.MapToKeys(channels)

	r := &Replica{
		id,
		channels,
		crdt,
		middleware.NewMiddleware(id, ids, channels, delay),
		communication.InitVClock(ids), //delivered version vector
	}

	go r.dequeue()

	return r
}

// Broadcasts a message by incrementing the replica's own entry in the version vector
// and enqueuing the message with the updated version vector to the middleware process.
func (r *Replica) TCBcast(msg communication.Message) {
	r.VersionVector[r.id]++
	r.TCDeliver(msg)
	r.middleware.Tcbcast <- msg
}

// Dequeues a message that is ready to be delivered to the replica process.
// Increments the sender's entry in the replica's version vector before calling the TCDeliver callback.
func (r *Replica) dequeue() {
	for {
		msg := <-r.middleware.DeliverCausal
		if msg.Type == communication.DLV {
			r.VersionVector[msg.OriginID] = msg.Version[msg.OriginID]
			r.TCDeliver(msg)
		} else if msg.Type == communication.STB {
			r.TCStable(msg)
		}
	}
}

// Update made by a client to a replica that receives the operation to be applied to the CRDT
// sends the operation to middleware for broadcast
func (r *Replica) Add(value any) {
	msg := communication.NewMessage(communication.DLV, communication.ADD, value, r.VersionVector.Copy(), r.id)
	r.TCBcast(msg)
}

func (r *Replica) Remove(value any) {
	msg := communication.NewMessage(communication.DLV, communication.REM, value, r.VersionVector.Copy(), r.id)
	r.TCBcast(msg)
}
