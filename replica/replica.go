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
	Effect(msg communication.Message)

	// The TCStable callback function is called when a message is set to stable.
	//stabilize
	Stabilize(msg communication.Message)

	// Query made by a client to a replica that returns the current state of the CRDT
	// after applying the unstable operations into the CRDT stable state
	Query() any
}

type Replica struct {
	crdt          CrdtI
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
		crdt:          crdt,
		replicas:      channels,
		middleware:    middleware.NewMiddleware(id, ids, channels),
		VersionVector: communication.InitVClock(ids), //delivered version vector
	}

	go r.dequeue()

	return r
}

// Broadcasts a message by incrementing the replica's own entry in the version vector
// and enqueuing the message with the updated version vector to the middleware process.
func (r *Replica) TCBcast(operation int, value any) {
	r.VersionVector.Tick(r.id)
	vv := r.VersionVector.Copy()
	msg := communication.NewMessage(communication.DLV, operation, value, vv, r.id)
	r.crdt.Effect(msg)
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
			r.crdt.Effect(msg)
		} else if msg.Type == communication.STB {
			//log.Println("[ REPLICA", r.id, "] STABILIZED ", msg, " FROM ", msg.OriginID)
			r.crdt.Stabilize(msg)
		}
	}
}

// Update made by a client to a replica that receives the operation to be applied to the CRDT
// sends the operation to middleware for broadcast
func (r *Replica) Add(value any) {
	/*if reflect.ValueOf(value).Kind() != reflect.Slice {
		if reflect.TypeOf(value) != reflect.TypeOf(r.State()) {
			fmt.Println("Error: Type mismatch")
			return
		}
	} else if reflect.TypeOf(value).Elem() != reflect.TypeOf(r.State()).Elem() {
		fmt.Println("Error: Type mismatch")
		return
	}*/

	r.TCBcast(communication.ADD, value)
}

func (r *Replica) Remove(value any) {
	r.TCBcast(communication.REM, value)
}

func (r *Replica) Query() any {
	return r.crdt.Query()
}

func (r *Replica) GetID() string {
	return r.id
}
