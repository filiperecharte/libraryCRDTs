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

// rga definition

type Vertex struct {
	Timestamp any
	Value     any
	OriginID  string
}

type RGA struct {
	Id string
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

			st = newVertices
		case "Rem":
			if msg.Value.(RGAOpValue).V.Timestamp == nil {
				return st
			}
			removeVertex := msg.Value.(RGAOpValue).V
			// find index where removed vertex can be found and clear its content to tombstone it
			index := indexOfVPtr(removeVertex, st)
			st[index] = Vertex{st[index].Timestamp, nil, st[index].OriginID}
		}
	}
	return st
}

func (r RGA) Stabilize(state any, op communication.Operation) any {
	//if operation is remove, remove the vertex from the state
	if op.Type == "Rem" {
		st := state.([]Vertex)
		if op.Value.(RGAOpValue).V.Timestamp == nil {
			return st
		}
		removeVertex := op.Value.(RGAOpValue).V
		index := indexOfVPtr(removeVertex, st)
		st = append(st[:index], st[index+1:]...)
		return st
	}
	return state
}

func (r RGA) Query(state any) any {
	//removes tombstones
	noTombs := []Vertex{}
	for i := 0; i < len(state.([]Vertex)); i++ {
		if state.([]Vertex)[i].Value != nil {
			noTombs = append(noTombs, state.([]Vertex)[i])
		}
	}
	return noTombs
}

// initialize RGA
func NewRGAReplica(id string, channels map[string]chan any, delay int) *replica.Replica {
	r := crdt.CommutativeStableCRDT{Data: &RGA{Id: id}, Stable_st: []Vertex{
		{nil, "", id},
	}}

	return replica.NewReplica(id, &r, channels, delay)
}

func indexOfVPtr(vertex Vertex, vertices []Vertex) int {
	for i, v := range vertices {
		if vertex.Timestamp == nil && v.Timestamp == nil || vertex.Timestamp == nil {
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

	at := vertices[offset]

	if at.OriginID < newVertex.OriginID || at.OriginID == newVertex.OriginID && at.Timestamp.(communication.VClock).Sum() < newVertex.Timestamp.(communication.VClock).Sum() {
		return offset
	}
	return shift(offset+1, newVertex, vertices)
}

// abstraction for test purposes

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
	vertex := vertices[index]
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
