package middleware

import (
	"fmt"
	"time"
)

type Middleware struct {
	replica          string
	channels         map[string]chan interface{}
	DeliveredVersion VClock // last delivered vector clock
	ReceivedVersion  VClock // last received vector clock
	Tcbcast          chan Message
	DeliverCausal    chan Message
	DQ               []Message
	Delay            bool
	//Unstable      map[interface{}]struct{} // Event operations waiting to stabilize
	//Observed      VClocks                  // vector versions of observed universe
}

// creates middleware state
func NewMiddleware(id string, ids []string, channels map[string]chan interface{}, delay bool) *Middleware {

	groupSize := len(ids)

	mw := &Middleware{
		replica:          id,
		channels:         channels,
		DeliveredVersion: InitVClock(ids, uint64(groupSize)),
		ReceivedVersion:  InitVClock(ids, uint64(groupSize)),
		Tcbcast:          make(chan Message),
		DeliverCausal:    make(chan Message),
		Delay:            delay,
		//Unstable:       make(map[interface{}]struct{}),
		//Observed:       make(VClocks),
	}

	go mw.dequeue()
	go mw.receive()

	return mw
}

// run middleware by waiting for messages on Tcbcast channel
func (mw *Middleware) dequeue() {
	for {
		msg := <-mw.Tcbcast
		mw.DeliveredVersion.Tick(mw.replica)
		mw.broadcast(msg)
	}
}

// TODO
// broadcasts a received message to other middlewares
// for testing purposes we will just call receive with the ids of the other middlewares
func (mw *Middleware) broadcast(msg Message) {
	for id, ch := range mw.channels {
		if mw.replica != id {
			go func(newCh chan interface{}) { newCh <- msg }(ch)
		}
	}
}

func (mw *Middleware) receive() {

	for {
		m1 := <-mw.channels[mw.replica]
		m := m1.(Message)

		if mw.Delay && m.OriginID == "3" {
			fmt.Println(mw.replica, "delaying: ", m)
			time.Sleep(10 * time.Second)
			go func() { mw.channels[mw.replica] <- m }()
			mw.Delay = false
			continue
		}

		V_m := m.Version
		j := m.OriginID

		if mw.ReceivedVersion[j] < V_m[j] { // messages from the same replica cannot be delivered out of order otherwise they are ignored
			mw.ReceivedVersion.Tick(j)
			if V_m[j] == mw.DeliveredVersion[j]+1 && allCausalPredecessorsDelivered(V_m, mw.DeliveredVersion, j) {
				mw.DeliveredVersion[j]++
				mw.DeliverCausal <- m
				mw.deliver()
			} else {
				mw.DQ = append(mw.DQ, m)
			}
		}
	}
}

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
			if msg.Version[msg.OriginID] == mw.DeliveredVersion[msg.OriginID]+1 && allCausalPredecessorsDelivered(msg.Version, mw.DeliveredVersion, msg.OriginID) {
				mw.DeliveredVersion[msg.OriginID]++
				mw.DeliverCausal <- NewMessage(DLV, msg.Value, msg.Version, msg.OriginID)
			} else {
				mw.DQ[from] = mw.DQ[to]
				to++
			}
			from++
		}
	}
}

func allCausalPredecessorsDelivered(V_m, V_i VClock, j string) bool {
	for k, v := range V_m {
		if k != j && v > V_i[k] {
			return false
		}
	}
	return true
}

/*
func (m *Middleware) stabilize() (stable map[interface{}]struct{}, stableVClock VClock) {
	stableVClock = m.Observed.Common()
	stable = make(map[interface{}]struct{})
	unstable := make(map[interface{}]struct{})

	for op, _ := range m.Unstable {
		cmp := op.(Event).Version.Compare(stableVClock)
		if cmp == Equal || cmp == Ancestor {
			stable[op] = struct{}{}
		} else {
			unstable[op] = struct{}{}
		}
	}

	// delete stable operations from unstable state
	m.Unstable = unstable

	//return stable operations to replica
	return
}

// prints middleware state
func (m Middleware) String() string {
	//improve string representation
	// TODO
	return "StableVersion: " + m.StableVersion.ReturnVCString() + "\n" +
		"LatestVersion: " + m.LatestVersion.ReturnVCString() + "\n"
}

*/
