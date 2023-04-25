package replica

import (
	"library/packages/communication"
	"library/packages/middleware"
	"library/packages/utils"
	"log"
)

//type ReplicaID string

type CrdtI interface {

	// The TCDeliver callback function is called when a message is ready to be delivered.
	//effect
	Effect(msg communication.Operation)

	// The TCStable callback function is called when a message is set to stable.
	//stabilize
	Stabilize(msg communication.Operation)

	// Query made by a client to a replica that returns the current state of the CRDT
	// after applying the unstable operations into the CRDT stable state
	Query() any
}

type Replica struct {
	Crdt          CrdtI
	id            string
	replicas      map[string]chan interface{}
	middleware    *middleware.Middleware
	VersionVector communication.VClock
}

func NewReplica(id string, crdt CrdtI, channels map[string]chan interface{}) *Replica {
	//initialize replica state

	ids := utils.MapToKeys(channels)

	r := &Replica{
		id:            id,
		Crdt:          crdt,
		replicas:      channels,
		middleware:    middleware.NewMiddleware(id, ids, channels),
		VersionVector: communication.InitVClock(ids), //delivered version vector
	}

	go r.dequeue()

	return r
}

// Broadcasts a message by incrementing the replica's own entry in the version vector
// and enqueuing the message with the updated version vector to the middleware process.
func (r *Replica) TCBcast(operation communication.Operation) {
	msg := communication.NewMessage(communication.DLV, operation.Type, operation.Value, operation.Version, r.id)
	r.Crdt.Effect(msg.Operation)
	r.middleware.Tcbcast <- msg
	log.Println("[ REPLICA", r.id, "] BROADCASTED", msg)
}

// Dequeues a message that is ready to be delivered to the replica process.
// Increments the sender's entry in the replica's version vector before calling the TCDeliver callback.
func (r *Replica) dequeue() {
	for {
		msg := <-r.middleware.DeliverCausal
		if msg.Type == communication.DLV {
			log.Println("[ REPLICA", r.id, "] RECEIVED ", msg, " FROM ", msg.OriginID)
			t := msg.Version.FindTicks(msg.OriginID)
			r.VersionVector.Set(msg.OriginID, t)
			r.Crdt.Effect(msg.Operation)
		} else if msg.Type == communication.STB {
			//log.Println("[ REPLICA", r.id, "] STABILIZED ", msg, " FROM ", msg.OriginID)
			r.Crdt.Stabilize(msg.Operation)
		}
	}
}

// Update made by a client to a replica that receives the operation to be applied to the CRDT
// sends the operation to middleware for broadcast
func (r *Replica) Prepare(operationType string, operationValue any){
	r.VersionVector.Tick(r.id)
	vv := r.VersionVector.Copy()
	op := communication.Operation{Type: operationType, Value: operationValue, Version: vv}
	r.TCBcast(op)
}

func (r *Replica) GetID() string {
	return r.id
}
