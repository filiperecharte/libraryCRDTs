package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
	"math"
)

type Position struct {
	i  int
	id string
}

type Vertex struct {
	ptr   Position
	value any
}

type Operation struct {
	after Position
	at    Position
	value any
}

type RGA struct {
	sequencer Position
}

func (r *RGA) Apply(state any, operations []any) any {
	st := state.([]Vertex)
	for _, op := range operations {
		msgOP := op.(communication.Message)
		switch msgOP.Type {
		case communication.ADD:
			// find index where predecessor vertex can be found
			predecessorIdx := indexOfVPtr(msgOP.Value.(Operation).after, st)
			// adjust index where new vertex is to be inserted when concurrent insertions for the same predecessor occur
			insertIdx := shift(predecessorIdx+1, msgOP.Value.(Operation).at, st)
			// update RGA to store the highest observed sequence number
			seqNr := r.sequencer.i
			replicaId := r.sequencer.id
			nextSeqNr := Position{int(math.Max(float64(msgOP.Value.(Operation).at.i), float64(seqNr))), replicaId}
			newVertex := Vertex{msgOP.Value.(Operation).at, msgOP.Value.(Operation).value}
			newVertices := append(st, Vertex{})
			copy(newVertices[insertIdx+1:], newVertices[insertIdx:])
			newVertices[insertIdx] = newVertex

			st = newVertices
			r.sequencer = nextSeqNr

			break
		case communication.REM:
			// find index where removed vertex can be found and clear its content to tombstone it
			index := indexOfVPtr(msgOP.Value.(Operation).at, st)
			st[index] = Vertex{msgOP.Value.(Operation).at, nil}

			break
		}
	}

	return st
}

// initialize RGA
func NewRGReplica(id string, channels map[string]chan any) *replica.Replica {
	r := crdt.CommutativeCRDT{Data: &RGA{}, Stable_st: []Vertex{}}

	return replica.NewReplica(id, &r, channels)
}

func indexOfVPtr(ptr Position, vertices []Vertex) int {
	for i, vertex := range vertices {
		if vertex.ptr == ptr {
			return i
		}
	}
	return -1
}

func shift(offset int, ptr Position, vertices []Vertex) int {
	if offset >= len(vertices) {
		return offset
	}
	next := vertices[offset].ptr
	if next.i < ptr.i {
		return offset
	}
	return shift(offset+1, ptr, vertices)
}
