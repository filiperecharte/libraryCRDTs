package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/datatypes"
	"library/packages/replica"
	"strconv"
)

type RGA datatypes.RGA

func (r RGA) Apply(state any, operations []communication.Operation) any {
	stCpy := RGACopy(state.([]datatypes.Vertex))

	for _, op := range operations {
		msg := op
		switch msg.Type {
		case "Add":
			newVertex := datatypes.Vertex{msg.Version, msg.Value.(datatypes.RGAOpValue).Value, msg.OriginID}
			newVertexPrev := msg.Value.(datatypes.RGAOpValue).V

			// find index where predecessor vertex can be found
			predecessorIdx := indexOfVPtr(newVertexPrev, stCpy)

			// if predecessor vertex is not found, insert on root
			if predecessorIdx == -1 {
				continue
			}

			newVertices := append(stCpy[:predecessorIdx+1], append([]datatypes.Vertex{newVertex}, stCpy[predecessorIdx+1:]...)...)

			stCpy = newVertices
		case "Rem":
			removeVertex := msg.Value.(datatypes.RGAOpValue).V
			// find index where removed vertex can be found and clear its content to tombstone it
			index := indexOfVPtr(removeVertex, stCpy)
			if index == -1 {
				continue
			}
			newVertices := append(stCpy[:index], stCpy[index+1:]...)
			stCpy = newVertices
		}
	}
	return stCpy
}

func (r RGA) Order(op1 communication.Operation, op2 communication.Operation) bool {
	id1, _ := strconv.Atoi(strconv.Itoa(int(op1.Version.Sum())) + op1.OriginID)
	id2, _ := strconv.Atoi(strconv.Itoa(int(op2.Version.Sum())) + op2.OriginID)

	return id1 < id2
}

func (r RGA) Commutes(op1 communication.Operation, op2 communication.Operation) bool {

	if op1.Type == "Add" && op2.Type == "Add" {
		return !op1.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock).Equal(op2.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock)) &&
			!op1.Version.Equal(op2.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock)) &&
			!op2.Version.Equal(op1.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock))
	}

	if op1.Type == "Rem" && op2.Type == "Add" {
		return !op1.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock).Equal(op2.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock)) &&
			!op1.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock).Equal(op2.Version)
	}

	if op1.Type == "Add" && op2.Type == "Rem" {
		return !op1.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock).Equal(op2.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock)) &&
			!op2.Value.(datatypes.RGAOpValue).V.Timestamp.(communication.VClock).Equal(op1.Version)
	}

	if op1.Type == "Rem" && op2.Type == "Rem" {
		return true
	}

	return false
}

// initialize RGA
func NewRGAReplica(id string, channels map[string]chan any, delay int) *replica.Replica {

	r := crdt.NewEcroCRDT(id, []datatypes.Vertex{{communication.NewVClockFromMap(map[string]uint64{}), "", id}}, RGA{id}, replica.Replica{})

	replica := replica.NewReplica(id, r, channels, delay)

	r.SetReplica(replica)

	return replica
}

func indexOfVPtr(vertex datatypes.Vertex, vertices []datatypes.Vertex) int {
	for i, v := range vertices {
		if vertex.Timestamp.(communication.VClock).Equal(v.Timestamp.(communication.VClock)) {
			return i
		}
	}
	return -1
}

// check if two array of vertices are equal
func RGAEqual(vertices1 []datatypes.Vertex, vertices2 []datatypes.Vertex) bool {
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

// deep copy state of RGA
func RGACopy(state []datatypes.Vertex) []datatypes.Vertex {
	stCpy := make([]datatypes.Vertex, len(state))
	for i, v := range state {
		stCpy[i].Value = v.Value
		stCpy[i].Timestamp = v.Timestamp.(communication.VClock).Copy()
		stCpy[i].OriginID = v.OriginID
	}
	return stCpy
}

func (r RGA) effectivePos(prevV datatypes.Vertex, state []datatypes.Vertex) datatypes.Vertex {
	for _, v := range state {
		if v.Timestamp.(communication.VClock).Equal(prevV.Timestamp.(communication.VClock)) {
			return prevV
		}
	}
	return datatypes.Vertex{communication.NewVClockFromMap(map[string]uint64{}), "", r.Id}
}
