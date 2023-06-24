package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
)

// initialize RGA
func NewRGAReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	r := crdt.NewSemidirectECRO(id, []Vertex{{communication.NewVClockFromMap(map[string]uint64{}), "", id}}, &RGA{})

	return replica.NewReplica(id, r, channels, delay)
}
