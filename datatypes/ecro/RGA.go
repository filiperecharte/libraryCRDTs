package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
	"log"
	"math"
)

type Position struct {
	I  int
	ID string
}

type Vertex struct {
	Ptr   Position
	Value any
}

type Operation struct {
	After Position
	At    Position
	Value any
}

type RGA struct {
	sequencer Position
}

func (r *RGA) Apply(state any, operations []communication.Operation) any {
	st := state.([]Vertex)
	for _, op := range operations {
		msgOP := op
		switch msgOP.Type {
		case "ADD":
			// find index where predecessor vertex can be found
			predecessorIdx := indexOfVPtr(msgOP.Value.(Operation).After, st)
			// adjust index where new vertex is to be inserted when concurrent insertions for the same predecessor occur
			insertIdx := shift(predecessorIdx+1, msgOP.Value.(Operation).At, st)
			// update RGA to store the highest observed sequence number
			seqNr := r.sequencer.I
			replicaId := r.sequencer.ID
			nextSeqNr := Position{int(math.Max(float64(msgOP.Value.(Operation).At.I), float64(seqNr))), replicaId}
			newVertex := Vertex{msgOP.Value.(Operation).At, msgOP.Value.(Operation).Value}
			newVertices := append(st, Vertex{})
			copy(newVertices[insertIdx+1:], newVertices[insertIdx:])
			newVertices[insertIdx] = newVertex

			st = newVertices
			r.sequencer = nextSeqNr

			break
		case "REM":
			log.Println("Received REM operation: ", msgOP.Value.(Operation).At)
			// find index where removed vertex can be found and clear its content to tombstone it
			index := indexOfVPtr(msgOP.Value.(Operation).At, st)
			st[index] = Vertex{msgOP.Value.(Operation).At, nil}

			break
		}
	}

	return st
}

// initialize RGA
func NewRGAReplica(id string, channels map[string]chan any, delay int) *replica.Replica {
	r := crdt.CommutativeCRDT{Data: &RGA{}, Stable_st: []Vertex{}}

	return replica.NewReplica(id, &r, channels, delay)
}

func indexOfVPtr(ptr Position, vertices []Vertex) int {
	for i, vertex := range vertices {
		if vertex.Ptr == ptr {
			return i
		}
	}
	return -1
}

func shift(offset int, ptr Position, vertices []Vertex) int {
	if offset >= len(vertices) {
		return offset
	}
	next := vertices[offset].Ptr
	if next.I < ptr.I {
		return offset
	}
	return shift(offset+1, ptr, vertices)
}
