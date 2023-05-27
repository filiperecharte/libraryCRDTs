package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
)

type RGAOpValue struct {
	V     Vertex //on an insert, the vertex to insert after, on a remove, the vertex to remove
	Value any
}

// operations
type Insert struct {
	Prev  Vertex
	Value any
}

type Remove struct {
	V Vertex
}

// rga definition
// the identifier must be any and come from the middleware

type Vertex struct {
	Timestamp any
	Value     any
	OriginID  string
}

type RGA struct {
	Vertices []Vertex
}

func (r *RGA) Apply(state any, operations []communication.Operation) any {
	st := state.([]Vertex)
	for _, op := range operations {
		msg := op
		switch msg.Type {
		case "Add":
			newVertex := Vertex{msg.Version, msg.Value.(RGAOpValue).Value, msg.OriginID}
			newVertexPrev := msg.Value.(RGAOpValue).V

			// find index where predecessor vertex can be found
			predecessorIdx := indexOfVPtr(newVertexPrev, st)

			// adjust index where new vertex is to be inserted when concurrent insertions for the same predecessor occur
			insertIdx := shift(predecessorIdx+1, newVertex, st)

			newVertices := append(st[:insertIdx], append([]Vertex{newVertex}, st[insertIdx:]...)...)
			r.Vertices = newVertices
			st = newVertices
		case "Rem":
			removeVertex := msg.Value.(RGAOpValue).V
			// find index where removed vertex can be found and clear its content to tombstone it
			index := indexOfVPtr(removeVertex, st)
			st[index] = Vertex{st[index].Timestamp, nil, st[index].OriginID}
		}
	}
	return st
}

// initialize RGA
func NewRGAReplica(id string, channels map[string]chan any, delay int) *replica.Replica {
	r := crdt.CommutativeCRDT{Data: &RGA{
		Vertices: []Vertex{
			{nil, "", id},
		},
	}, Stable_st: []Vertex{
		{nil, "", id},
	}}

	return replica.NewReplica(id, &r, channels, delay)
}

func indexOfVPtr(vertex Vertex, vertices []Vertex) int {
	for i, v := range vertices {
		if vertex.Timestamp == nil && v.Timestamp == nil {
			return 0
		} else if v.Timestamp == nil {
			continue
		}
		if vertex.Timestamp.(communication.VClock).Equal(v.Timestamp.(communication.VClock)) {
			return i
		}
	}
	return -1
}

func shift(offset int, newVertex Vertex, vertices []Vertex) int {
	if offset >= len(vertices) {
		return offset
	}
	next := vertices[offset]
	concurrent := newVertex.Timestamp.(communication.VClock).Compare(next.Timestamp.(communication.VClock)) == communication.Concurrent
	if !concurrent || (concurrent && next.OriginID < newVertex.OriginID) {
		return offset
	}
	return shift(offset+1, newVertex, vertices)
}

// check if vertices have a vertex
func HasVertex(vertices []Vertex, vertex Vertex) bool {
	for _, v := range vertices {
		if vertex.Timestamp == nil && v.Timestamp == nil {
			return true
		} else if v.Timestamp == nil {
			continue
		}
		if v.Timestamp.(communication.VClock).Equal(vertex.Timestamp.(communication.VClock)) && v.Value == vertex.Value {
			return true
		}
	}
	return false
}

// abstraction for the RGA operations

type RGAOpIndex struct {
	Index int
	Value any
}

func GetPrevVertex(index int, vertices []Vertex) Vertex {
	//index := indexWithTombstones(i, rga.Vertices)
	prev := vertices[index]
	return prev
}

func GetVertexRemove(index int, vertices []Vertex) Vertex {
	//index := indexWithTombstones(i, rga.Vertices)
	vertex := vertices[index+1]
	return vertex
}

// check if two array of vertices are equal
func RGAEqual(vertices1 []Vertex, vertices2 []Vertex) bool {
	if len(vertices1) != len(vertices2) {
		return false
	}
	for i, v := range vertices1 {
		if v.Timestamp != nil && vertices2[i].Timestamp == nil {
			return false
		} else if v.Timestamp == nil && vertices2[i].Timestamp != nil {
			return false
		} else if v.Timestamp == nil && vertices2[i].Timestamp == nil {
			continue
		} else if !v.Timestamp.(communication.VClock).Equal(vertices2[i].Timestamp.(communication.VClock)) || v.Value != vertices2[i].Value {
			return false
		}
	}
	return true
}
