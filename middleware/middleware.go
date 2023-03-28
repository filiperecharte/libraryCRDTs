package middleware

import (
	"fmt"
	"library/packages/utils"
	"sort"
)

type StableDotKey struct {
	id string
	Vm uint64
}

type StableDotValue struct {
	msg Message
	ctr uint64
}

type Middleware struct {
	replica          string
	channels         map[string]chan interface{}
	groupSize        int
	DeliveredVersion VClock // last delivered vector clock
	ReceivedVersion  VClock // last received vector clock
	Tcbcast          chan Message
	DeliverCausal    chan Message
	DQ               []Message
	Delay            bool
	Observed         VClocks // vector versions of observed universe
	StableVersion    VClock
	SMap             map[StableDotKey]StableDotValue // messages delivered but not yet stable (stable dots)
	Min              map[string]string
	Ctr              uint64
}

// creates middleware state
func NewMiddleware(id string, ids []string, channels map[string]chan interface{}, delay bool) *Middleware {

	groupSize := len(ids)

	mw := &Middleware{
		replica:          id,
		channels:         channels,
		groupSize:        groupSize,
		DeliveredVersion: InitVClock(ids),
		ReceivedVersion:  InitVClock(ids),
		Tcbcast:          make(chan Message),
		DeliverCausal:    make(chan Message),
		Delay:            delay,
		Observed:         InitVClocks(ids),
		StableVersion:    InitVClock(ids),
		SMap:             make(map[StableDotKey]StableDotValue),
		Min:              utils.InitMin(ids),
		Ctr:              0,
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
		mw.updatestability(msg)
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

		/*if mw.Delay && m.OriginID == "3" {
			fmt.Println(mw.replica, "delaying: ", m)
			time.Sleep(10 * time.Second)
			go func() { mw.channels[mw.replica] <- m }()
			mw.Delay = false
			continue
		}*/

		V_m := m.Version
		j := m.OriginID

		if mw.ReceivedVersion[j] < V_m[j] { // messages from the same replica cannot be delivered out of order otherwise they are ignored
			mw.ReceivedVersion.Tick(j)
			if V_m[j] == mw.DeliveredVersion[j]+1 && allCausalPredecessorsDelivered(V_m, mw.DeliveredVersion, j) {
				mw.DeliveredVersion[j]++
				mw.DeliverCausal <- m
				mw.updatestability(m)
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

func (mw *Middleware) updatestability(msg Message) {
	mw.Observed[mw.replica] = mw.DeliveredVersion
	if mw.replica != msg.OriginID {
		mw.Observed[msg.OriginID] = msg.Version
	}
	mw.Ctr++
	mw.SMap[StableDotKey{msg.OriginID, msg.Version[msg.OriginID]}] = StableDotValue{msg, mw.Ctr}
	if _, ok := mw.Min[msg.OriginID]; ok {
		var NewStableVersion = mw.calculateStableVersion(msg.OriginID)
		if NewStableVersion.Compare(mw.StableVersion) != Equal {
			StableDots := NewStableVersion.Subtract(mw.StableVersion)
			mw.stabilize(StableDots)
			mw.StableVersion = NewStableVersion.Copy()
		}
	}
}

func (mw *Middleware) stabilize(StableDots VClock) {
	var L []StableDotValue
	for k, _ := range StableDots {
		if _, ok := mw.SMap[StableDotKey{k, StableDots[k]}]; ok {
			L = append(L, mw.SMap[StableDotKey{k, StableDots[k]}])
		}
	}
	sort.Slice(L, func(i, j int) bool {
		return L[i].ctr < L[j].ctr
	})

	fmt.Println("L: ", L)

	for _, stableDot := range L {
		stableDot.msg.SetType(STB)
		mw.DeliverCausal <- stableDot.msg
	}
	//removes stable dots from SMap
	for k, _ := range StableDots {
		delete(mw.SMap, StableDotKey{k, StableDots[k]})
	}
}

func (mw *Middleware) calculateStableVersion(j string) VClock {
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
