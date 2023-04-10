package middleware

import (
	"library/packages/communication"
	"library/packages/utils"
	"log"
	"sort"
)

type StableDotKey struct {
	id string
	Vm uint64
}

type StableDotValue struct {
	msg communication.Message
	ctr uint64
}

type SMap map[StableDotKey]StableDotValue

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
	Min              map[string]string           // Replicas with the min vector
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
		SMap:             make(map[StableDotKey]StableDotValue),
		Min:              utils.InitMin(ids),
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
		log.Println("replica ", mw.replica, " received ", m1, " from ", m1.(communication.Message).OriginID)
		m := m1.(communication.Message)

		V_m := m.Version
		j := m.OriginID

		//if mw.ReceivedVersion[j] < V_m[j] { // communication.Messages from the same replica cannot be delivered out of order otherwise they are ignored
		mw.ReceivedVersion.Tick(j)
		if V_m[j] == mw.DeliveredVersion[j]+1 && allCausalPredecessorsDelivered(V_m, mw.DeliveredVersion, j) {
			mw.DeliveredVersion[j]++
			mw.DeliverCausal <- m
			mw.updatestability(m)
			mw.deliver()
		} else {
			log.Println("replica", mw.replica, "adding to DQ: ", m)
			mw.DQ = append(mw.DQ, m)
		}
		//}
	}
}

// checks DQ to see if new messages can be delivered
func (mw *Middleware) deliver() {
	log.Println("replica", mw.replica, "delivering from DQ: ", mw.DQ)
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
			if msg.Version[msg.OriginID] == mw.DeliveredVersion[msg.OriginID]+1 && allCausalPredecessorsDelivered(msg.Version, mw.DeliveredVersion, msg.OriginID) {
				mw.DeliveredVersion[msg.OriginID]++
				mw.DeliverCausal <- communication.NewMessage(communication.DLV, msg.Operation, msg.Value, msg.Version, msg.OriginID)
			} else {
				mw.DQ[from] = mw.DQ[to]
				to++
			}
			from++
		}
	}
}

// check if a message has his causal predecessors delivered
func allCausalPredecessorsDelivered(V_m, V_i communication.VClock, j string) bool {
	for k, v := range V_m {
		if k != j && v > V_i[k] {
			return false
		}
	}
	return true
}

// Updates observed matrix and counter, finds stable version and send stable messages
func (mw *Middleware) updatestability(msg communication.Message) {

	mw.Observed[mw.replica] = mw.DeliveredVersion
	if mw.replica != msg.OriginID {
		mw.Observed[msg.OriginID] = msg.Version
	}

	mw.Ctr++

	mw.SMap[StableDotKey{msg.OriginID, msg.Version[msg.OriginID]}] = StableDotValue{msg, mw.Ctr}

	if _, ok := mw.Min[msg.OriginID]; ok {
		var NewStableVersion = mw.calculateStableVersion(msg.OriginID)
		if NewStableVersion.Compare(mw.StableVersion) != communication.Equal {
			StableDots := NewStableVersion.Subtract(mw.StableVersion)
			mw.stabilize(StableDots)
			mw.StableVersion = NewStableVersion.Copy()
		}
	}
}

// Order messages on stable dots and send the ones before stable vector to replica
func (mw *Middleware) stabilize(StableDots communication.VClock) {
	var L []StableDotValue
	for k, _ := range StableDots {

		if _, ok := mw.SMap[StableDotKey{k, StableDots[k]}]; ok {
			L = append(L, mw.SMap[StableDotKey{k, StableDots[k]}])
		}

	}
	sort.Slice(L, func(i, j int) bool {
		return L[i].ctr < L[j].ctr
	})

	for _, stableDot := range L {
		stableDot.msg.SetType(communication.STB)
		mw.DeliverCausal <- stableDot.msg
	}
	//removes stable dots from SMap
	for k, _ := range StableDots {
		delete(mw.SMap, StableDotKey{k, StableDots[k]})
	}
}

// Calculating the Stable vector every time Observed is updated can become costly, specially when dealing with large groups.
// To overcome this problem the Min vector was created, by checking if the sender’s id is in it.
// If it is not, then the minimums of the columns are the same and Min has not changed.
func (mw *Middleware) calculateStableVersion(j string) communication.VClock {
	newStableVersion := mw.StableVersion.Copy()
	for keyMin, _ := range mw.Min {
		if keyMin == j {

			min := mw.Observed[keyMin][keyMin]
			minRow := keyMin
			for keyObs, _ := range mw.Observed {
				if mw.Observed[keyObs][keyMin] < min {
					min = mw.Observed[keyObs][keyMin]
					minRow = keyObs
				}
			}

			newStableVersion[keyMin] = min
			mw.Min[keyMin] = minRow
		}

	}
	return newStableVersion
}
