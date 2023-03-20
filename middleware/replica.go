package middleware

type ReplicaID string

// Matrix of vector clocks for each replica
type VClocks map[ReplicaID]VClock

// returns vector clock that is common to all replicas by choosing the minimum value of each vector clock
func (vc VClocks) Common() VClock {
	if len(vc) == 0 {
		return nil
	}
	common := make(VClock)
	for _, vclock := range vc {
		for id, ticks := range vclock {
			if common[id] > ticks {
				common[id] = ticks
			}
		}
	}
	return common
}

// returns the latest vector clock (most up to date) by choosing the maximum value of each vector clock
func (vc VClocks) Latest() VClock {
	if len(vc) == 0 {
		return nil
	}
	latest := make(VClock)
	for _, vclock := range vc {
		for id, ticks := range vclock {
			if latest[id] < ticks {
				latest[id] = ticks
			}
		}
	}
	return latest
}

// merges two matrix of vector clocks together by choosing the maximum value of each vector clock
func (vc VClocks) Merge(other VClocks) {
	for id, vclock := range other {
		if _, ok := vc[id]; !ok {
			vc[id] = make(VClock)
		}
		vc[id].Merge(vclock)
	}
}

// updates the matrix by adding a new vector clock for a replica if it does not exist and merge if it exists
func (vc VClocks) Update(id ReplicaID, vclock VClock) {
	if _, ok := vc[id]; !ok {
		vc[id] = make(VClock)
	}
	vc[id].Merge(vclock)
}
