package middleware

type Middleware struct {
	replica          string
	replicas         []string
	Middlewares      []Middleware
	DeliveredVersion VClock // last delivered vector clock
	ReceivedVersion  VClock // last received vector clock
	Tcbcast          chan Message
	DeliverCausal    chan Message
	DQ               []Message
	//Unstable      map[interface{}]struct{} // Event operations waiting to stabilize
	//Observed      VClocks                  // vector versions of observed universe
}

// creates middleware state
func NewMiddleware(id string, ids []string, groupSize int) *Middleware {
	mw := &Middleware{
		replica:          id,
		replicas:         ids,
		DeliveredVersion: InitVClock(ids, uint64(groupSize)),
		ReceivedVersion:  InitVClock(ids, uint64(groupSize)),
		Tcbcast:          make(chan Message),
		DeliverCausal:    make(chan Message),
		//Unstable:       make(map[interface{}]struct{}),
		//Observed:       make(VClocks),
	}

	go mw.dequeue()

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
	for _, md := range mw.Middlewares {
		if md.replica != mw.replica {
			md.receive(mw.replica, msg)
		}
	}
}

func (mw *Middleware) receive(j string, m Message) {
	V_m := m.Version
	if mw.ReceivedVersion[j] < V_m[j] {
		mw.ReceivedVersion.Tick(j)
		if V_m[j] == mw.DeliveredVersion[j]+1 && allCausalPredecessorsDelivered(V_m, mw.DeliveredVersion, j) {
			mw.DeliverCausal <- *NewMessage(m, V_m, j)
			go mw.deliver()
		} else {
			mw.DQ = append(mw.DQ, m)
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
				mw.DeliverCausal <- msg
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

//-------------------- Protocol --------------------//

// SUBMIT operation that creates a new event with an incremented version, adds the event to unstable state
// and sends it to all replicas.
func (m *Middleware) Update(replicaID string, state *Middleware, operation interface{}) {
	// increment version
	state.LatestVersion.Tick(replicaID)

	//increment observed version
	state.Observed.Update(replicaID, state.LatestVersion)

	//creates an event
	event := new(Event)
	event.Origin = replicaID
	event.Version = state.LatestVersion
	event.Timestamp = time.Now()
	event.Value = operation

	// add event to unstable state
	state.Unstable[event] = struct{}{}

	// broadcast event to all replicas
	// TODO

	// call stabilize to see if there are any stable operations?

	// do the previous two steps concurrently
	// TODO
}
*/
