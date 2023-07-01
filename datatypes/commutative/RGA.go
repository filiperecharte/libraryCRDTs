package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/datatypes"
	"library/packages/replica"
	"strconv"
)

type RGA datatypes.RGA

func (r *RGA) Apply(state any, operations []communication.Operation) any {
	st := state.([]datatypes.Vertex)
	for _, op := range operations {
		msg := op
		switch msg.Type {
		case "Add":
			newVertex := datatypes.Vertex{msg.Version, msg.Value.(datatypes.RGAOpValue).Value, msg.OriginID}
			newVertexPrev := msg.Value.(datatypes.RGAOpValue).V

			// find index where predecessor vertex can be found
			predecessorIdx := indexOfVPtr(newVertexPrev, st)

			// if predecessor vertex is not found, insert on root
			if predecessorIdx == -1 {
				predecessorIdx = 0
			}

			// adjust index where new vertex is to be inserted when concurrent insertions for the same predecessor occur
			insertIdx := shift(predecessorIdx+1, newVertex, st)

			newVertices := append(st[:insertIdx], append([]datatypes.Vertex{newVertex}, st[insertIdx:]...)...)

			st = newVertices
		case "Rem":
			removeVertex := msg.Value.(datatypes.RGAOpValue).V
			// find index where removed vertex can be found and clear its content to tombstone it
			index := indexOfVPtr(removeVertex, st)
			if index == -1 {
				continue
			}
			st[index] = datatypes.Vertex{st[index].Timestamp, nil, st[index].OriginID}
		}
	}
	return st
}

func (r RGA) Stabilize(state any, op communication.Operation) any {
	//if operation is remove, remove the vertex from the state
	if op.Type == "Rem" {
		st := state.([]datatypes.Vertex)
		removeVertex := op.Value.(datatypes.RGAOpValue).V
		index := indexOfVPtr(removeVertex, st)
		if index == -1 {
			return state
		}
		st = append(st[:index], st[index+1:]...)
		return st
	}
	return state
}

func (r RGA) Query(state any) any {
	//removes tombstones
	noTombs := []datatypes.Vertex{}
	for i := 0; i < len(state.([]datatypes.Vertex)); i++ {
		if state.([]datatypes.Vertex)[i].Value != nil {
			noTombs = append(noTombs, state.([]datatypes.Vertex)[i])
		}
	}
	return noTombs
}

// initialize RGA
func NewRGAReplica(id string, channels map[string]chan any, delay int) *replica.Replica {
	r := crdt.CommutativeStableCRDT{Data: &RGA{Id: id}, Stable_st: []datatypes.Vertex{{communication.NewVClockFromMap(map[string]uint64{}), "", id}}}

	return replica.NewReplica(id, &r, channels, delay)
}

func indexOfVPtr(vertex datatypes.Vertex, vertices []datatypes.Vertex) int {
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

func shift(offset int, newVertex datatypes.Vertex, vertices []datatypes.Vertex) int {
	if offset >= len(vertices) {
		return offset
	}

	at := vertices[offset]

	id1, _ := strconv.Atoi(strconv.Itoa(int(at.Timestamp.(communication.VClock).Sum())) + at.OriginID)
	id2, _ := strconv.Atoi(strconv.Itoa(int(newVertex.Timestamp.(communication.VClock).Sum())) + newVertex.OriginID)
	if id1 < id2 {
		return offset
	}
	return shift(offset+1, newVertex, vertices)
}
