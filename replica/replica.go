package replica

import (
	"library/packages/communication"
	"library/packages/middleware"
	"library/packages/utils"
	"log"
	"sync"
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
	Query() (any, any)

	// Returns the number of operations applied to the CRDT for testing purposes
	NumOps() uint64
}

type Replica struct {
	Crdt          CrdtI
	id            string
	replicas      map[string]chan interface{}
	middleware    *middleware.Middleware
	VersionVector communication.VClock
	prepareLock   *sync.RWMutex

	quit chan bool
}

func NewReplica(id string, crdt CrdtI, channels map[string]chan interface{}, delay int) *Replica {
	//initialize replica state

	ids := utils.MapToKeys(channels)

	r := &Replica{
		id:            id,
		Crdt:          crdt,
		replicas:      channels,
		middleware:    middleware.NewMiddleware(id, ids, channels, delay),
		VersionVector: communication.InitVClock(ids), //delivered version vector
		prepareLock:   new(sync.RWMutex),

		quit: make(chan bool),
	}

	go r.dequeue()

	return r
}

// quits goroutines
func (r *Replica) Quit() {
	r.middleware.Quit()
	r.quit <- true
}

// Broadcasts a message by incrementing the replica's own entry in the version vector
// and enqueuing the message with the updated version vector to the middleware process.
func (r *Replica) TCBcast(msg communication.Message) {
	log.Println("[ REPLICA", r.id, "] BROADCASTED", msg)
	r.middleware.Tcbcast <- msg
}

// Dequeues a message that is ready to be delivered to the replica process.
// Increments the sender's entry in the replica's version vector before calling the TCDeliver callback.
func (r *Replica) dequeue() {
	for {
		select {
		case <-r.quit:
			return
		default:
			msg := <-r.middleware.DeliverCausal
			if msg.Type == communication.DLV {
				log.Println("[ REPLICA", r.id, "] RECEIVED ", msg, " FROM ", msg.OriginID)
				r.prepareLock.Lock()
				if msg.OriginID != r.id {
					t := msg.Version.FindTicks(msg.OriginID)
					r.VersionVector.Set(msg.OriginID, t)
				}
				r.Crdt.Effect(msg.Operation)
				r.prepareLock.Unlock()
			} else if msg.Type == communication.STB {
				log.Println("[ REPLICA", r.id, "] STABILIZED ", msg, " FROM ", msg.OriginID)
				r.Crdt.Stabilize(msg.Operation)
			}
		}
	}
}

// Update made by a client to a replica that receives the operation to be applied to the CRDT
// sends the operation to middleware for broadcast
func (r *Replica) Prepare(operationType string, operationValue any) communication.Operation {
	r.prepareLock.Lock()
	r.VersionVector.Tick(r.id)
	vv := r.VersionVector.Copy()
	op := communication.Operation{Type: operationType, Value: operationValue, Version: vv, OriginID: r.id}
	msg := communication.NewMessage(communication.DLV, op.Type, op.Value, op.Version, op.OriginID)
	//r.Crdt.Effect(msg.Operation)
	r.prepareLock.Unlock()

	r.TCBcast(msg)

	return op //for testing purposes
}

func (r *Replica) GetID() string {
	return r.id
}
