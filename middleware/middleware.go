package middleware

import (
	"library/packages/communication"
	"library/packages/utils"
	"log"
	"sort"
	"sync"
)

type StableDotKey struct {
	id string
	Vm uint64
}

type StableDotValue struct {
	msg communication.Message
	ctr uint64
}

type SMap struct {
	*sync.RWMutex
	m map[StableDotKey]StableDotValue
}

type Min struct {
	*sync.RWMutex
	m map[string]string
}

type Middleware struct {
	replica          string                      // replica id
	channels         map[string]chan interface{} // all channels of the universe
	groupSize        int                         // size of the universe
	DeliveredVersion communication.VClock        // last delivered vector clock
	ReceivedVersion  communication.VClock        // last received vector clock
	Tcbcast          chan communication.Message  // channel to receive messages from replica
	DeliverCausal    chan communication.Message  // channel to causal deliver messages to replica
	DQ               []communication.Message     // Delivery queue to add messages that dont have causal predecessors yet
	Delay            bool                        // delay receive of message for debug reasons
	Observed         VClocks                     // vector versions of observed universe
	StableVersion    communication.VClock        // stable vector version
	SMap             SMap                        // Messages delivered to replica but not yet stable (stable dots)
	Min              Min                         // Replicas with the min vector
	Ctr              uint64                      // order messages on stable delivery
}

// creates middleware state
func NewMiddleware(id string, ids []string, channels map[string]chan interface{}) *Middleware {

	groupSize := len(ids)

	mw := &Middleware{
		replica:          id,
		channels:         channels,
		groupSize:        groupSize,
		DeliveredVersion: communication.InitVClock(ids),
		ReceivedVersion:  communication.InitVClock(ids),
		Tcbcast:          make(chan communication.Message),
		DeliverCausal:    make(chan communication.Message),
		Observed:         InitVClocks(ids),
		StableVersion:    communication.InitVClock(ids),
		SMap:             SMap{RWMutex: new(sync.RWMutex), m: make(map[StableDotKey]StableDotValue)},
		Min:              Min{RWMutex: new(sync.RWMutex), m: utils.InitMin(ids)},
		Ctr:              0,
	}

	go mw.dequeue()
	go mw.receive()

	return mw
}

// run middleware by waiting for communication.Messages on Tcbcast channel
func (mw *Middleware) dequeue() {
	for {
		msg := <-mw.Tcbcast
		mw.DeliveredVersion.Tick(mw.replica)
		mw.updatestability(msg)
		mw.broadcast(msg)
	}
}

// TODO
// broadcasts a received communication.Message to other middlewares
// for testing purposes we will just call receive with the ids of the other middlewares
func (mw *Middleware) broadcast(msg communication.Message) {
	for id, ch := range mw.channels {
		if mw.replica != id {
			go func(newCh chan interface{}) { newCh <- msg }(ch)
		}
	}
}

// receive messages from other replicas
func (mw *Middleware) receive() {
	for {
		m1 := <-mw.channels[mw.replica]
		m := m1.(communication.Message)
		(m.Version).NewMutex() //because messages save pointers do mutexes

		V_m := m.Version
		j := m.OriginID

		//if mw.ReceivedVersion[j] < V_m[j] { // communication.Messages from the same replica cannot be delivered out of order otherwise they are ignored
		mw.ReceivedVersion.Tick(j)
		if V_m.FindTicks(j) == mw.DeliveredVersion.FindTicks(j)+1 && allCausalPredecessorsDelivered(V_m, mw.DeliveredVersion, j) {
			mw.DeliveredVersion.Tick(j)
			mw.DeliverCausal <- m
			mw.updatestability(m)
			mw.deliver()
		} else {
			mw.DQ = append(mw.DQ, m)
		}
		//}
	}
}

// checks DQ to see if new messages can be delivered
func (mw *Middleware) deliver() {
	from := 0
	to := 0
	for {
		if from >= len(mw.DQ) {
			if to >= from {
				break
			}
			mw.DQ = mw.DQ[:to]
			if len(mw.DQ) == 0 {
				break
			}
			from = 0
			to = 0
		} else {
			msg := mw.DQ[from]
			if msg.Version.FindTicks(msg.OriginID) == mw.DeliveredVersion.FindTicks(msg.OriginID)+1 && allCausalPredecessorsDelivered(msg.Version, mw.DeliveredVersion, msg.OriginID) {
				mw.DeliveredVersion.Tick(msg.OriginID)
				msg := communication.NewMessage(communication.DLV, msg.Operation, msg.Value, msg.Version, msg.OriginID)
				mw.DeliverCausal <- msg
				mw.updatestability(msg)
			} else {
				mw.DQ[to] = mw.DQ[from]
				to++
			}
			from++
		}
	}
}

// check if a message has his causal predecessors delivered
func allCausalPredecessorsDelivered(V_m, V_i communication.VClock, j string) bool {
	for k, v := range V_m.GetMap() {
		if k != j && v > V_i.FindTicks(k) {
			return false
		}
	}
	return true
}

// Updates observed matrix and counter, finds stable version and send stable messages
func (mw *Middleware) updatestability(msg communication.Message) {
	mw.Observed.SetVClock(mw.replica, mw.DeliveredVersion) //updates current replica with its own version
	if mw.replica != msg.OriginID {
		mw.Observed.SetVClock(msg.OriginID, msg.Version) //updates observed matrix with the version of the received message
	}
	mw.Ctr++

	//delivered messages but not yet stable are stored in SMap
	mw.SMap.Lock()
	mw.SMap.m[StableDotKey{msg.OriginID, msg.Version.FindTicks(msg.OriginID)}] = StableDotValue{msg, mw.Ctr}
	mw.SMap.Unlock()

	// Min is a map of ids (rows) that contain the id of the replica (column) with the minimum version in that row
	//check if sender is in Min, if it is not, then the minimums of the columns are the same and SV hasn't change
	mw.Min.Lock()
	ok := utils.MapValueExists(mw.Min.m, msg.OriginID)
	mw.Min.Unlock()

	if ok {
		var NewStableVersion = mw.calculateStableVersion(msg.OriginID)
		if !NewStableVersion.Equal(mw.StableVersion) {
			StableDots := NewStableVersion.Subtract(mw.StableVersion)
			mw.stabilize(StableDots)
			mw.StableVersion = NewStableVersion.Copy()
		}
	}
}

// Order messages on stable dots and send the ones before stable vector to replica
func (mw *Middleware) stabilize(StableDots communication.VClock) {
	var L []StableDotValue
	for k, t := range StableDots.GetMap() {

		mw.SMap.Lock()
		if _, ok := mw.SMap.m[StableDotKey{k, t}]; ok {
			L = append(L, mw.SMap.m[StableDotKey{k, t}])
		}
		mw.SMap.Unlock()

	}
	sort.Slice(L, func(i, j int) bool {
		return L[i].ctr < L[j].ctr
	})
	for _, stableDot := range L {
		stableDot.msg.SetType(communication.STB)
		mw.DeliverCausal <- stableDot.msg
		log.Println("REPLICA", mw.replica, "STABLE MESSAGE ", stableDot.msg)
	}
	//removes stable dots from SMap
	for k, t := range StableDots.GetMap() {
		mw.SMap.Lock()
		delete(mw.SMap.m, StableDotKey{k, t})
		mw.SMap.Unlock()
	}
}

// Calculating the Stable vector every time Observed is updated can become costly, specially when dealing with large groups.
// To overcome this problem the Min vector was created, by checking if the sender’s id is in it.
// If it is not, then the minimums of the columns are the same and Min has not changed.
func (mw *Middleware) calculateStableVersion(j string) communication.VClock {
	newStableVersion := mw.StableVersion.Copy()
	min := mw.Min
	for keyMin, _ := range min.m {
		if keyMin == j {
			min := mw.Observed.GetTick(j, keyMin)
			minRow := keyMin

			obs := mw.Observed.GetMap()

			mw.Observed.Lock()
			for keyObs, _ := range obs {
				if mw.Observed.m[keyObs].FindTicks(keyMin) < min {
					min = mw.Observed.m[keyObs].FindTicks(keyMin)
					minRow = keyObs
				}
			}
			mw.Observed.Unlock()

			newStableVersion.Set(keyMin, min)

			mw.Min.Lock()
			mw.Min.m[keyMin] = minRow
			mw.Min.Unlock()
		}
	}
	return newStableVersion
}
