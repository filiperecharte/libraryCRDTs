package crdts

import (
	"fmt"
	"library/packages/communication"
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
	state     []Vertex
	sequencer Position
}

func (r *RGA) TCDeliver(msg communication.Message) {
	switch msg.Type {
	case communication.ADD:
		// find index where predecessor vertex can be found
		predecessorIdx := indexOfVPtr(msg.Value.(Operation).after, r.state)
		// adjust index where new vertex is to be inserted when concurrent insertions for the same predecessor occur
		insertIdx := shift(predecessorIdx+1, msg.Value.(Operation).at, r.state)
		// update RGA to store the highest observed sequence number
		seqNr := r.sequencer.i
		replicaId := r.sequencer.id
		nextSeqNr := Position{int(math.Max(float64(msg.Value.(Operation).at.i), float64(seqNr))), replicaId}
		newVertex := Vertex{msg.Value.(Operation).at, msg.Value.(Operation).value}
		newVertices := append(r.state, Vertex{})
		copy(newVertices[insertIdx+1:], newVertices[insertIdx:])
		newVertices[insertIdx] = newVertex

		r.state = newVertices
		r.sequencer = nextSeqNr

		break
	case communication.REM:
		// find index where removed vertex can be found and clear its content to tombstone it
		index := indexOfVPtr(msg.Value.(Operation).at, r.state)
		r.state[index] = Vertex{msg.Value.(Operation).at, nil}

		break
	}
}

func (r *RGA) TCStable(msg communication.Message) {
	fmt.Println("Ignoring received stable operation: ", msg)
}

func (r *RGA) Query() any {
	return r.state
}

// initialize counter
func NewRGA(id string, channels map[string]chan any) *replica.Replica {
	r := &RGA{
		state: make([]Vertex, 0),
	}

	return replica.NewReplica(id, r, channels)
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
