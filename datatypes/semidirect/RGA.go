package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
	"log"
	"strconv"
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

func (r RGA) Apply(state any, operations []communication.Operation) any {
	stCpy := state.([]Vertex)

	for _, op := range operations {
		msg := op
		switch msg.Type {
		case "Add":
			newVertex := Vertex{msg.Version, msg.Value.(RGAOpValue).Value, msg.OriginID}
			newVertexPrev := msg.Value.(RGAOpValue).V

			// find index where predecessor vertex can be found
			predecessorIdx := indexOfVPtr(newVertexPrev, stCpy)

			newVertices := append(stCpy[:predecessorIdx+1], append([]Vertex{newVertex}, stCpy[predecessorIdx+1:]...)...)

			stCpy = newVertices
		case "Rem":
			removeVertex := msg.Value.(RGAOpValue).V
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

func (r RGA) ArbitrationOrder(op1 communication.Operation, op2 communication.Operation) (bool, bool) {
	//log.Println(r.Id, "ARBITRATIONORDER", op1, op2)

	//verifies if the two operations are inserts after the same Vertex, if yes order by operation id (timestamp - vectorclock) -> will need repair
	if op1.Value.(RGAOpValue).V.Timestamp.(communication.VClock).Equal(op2.Value.(RGAOpValue).V.Timestamp.(communication.VClock)) {
		//arbitration order by ids
		id1, _ := strconv.Atoi(strconv.Itoa(int(op1.Version.Sum())) + op1.OriginID)
		id2, _ := strconv.Atoi(strconv.Itoa(int(op2.Version.Sum())) + op2.OriginID)
		return false, id1 < id2
		//if the insert is not after the same vertex:
	} else {
		//check if one of them is the previous vertex of another, if yes order by causality,
		if op1.Version.Equal(op2.Value.(RGAOpValue).V.Timestamp.(communication.VClock)) {
			return false, true
			//if no, they are commutative and we can order them by any rule (e.g. ids)
		} else {
			return true, true
		}
	}
}

func (r RGA) Repair(op1 communication.Operation, op2 communication.Operation) communication.Operation {

	_, ordered := r.ArbitrationOrder(op1, op2)
	if !ordered {
		return communication.Operation{
			Type:    op2.Type,
			Version: op2.Version,
			Value: RGAOpValue{
				Vertex{
					Timestamp: op1.Version,
					OriginID:  op1.OriginID,
				},
				op2.Value.(RGAOpValue).Value,
			},
			OriginID: op2.OriginID,
		}
	}
	return op2
}

func (r RGA) RepairCausal(op1 communication.Operation, op2 communication.Operation) communication.Operation {

	if op1.Type == "Rem" && op2.Type == "Add" &&
		op1.Value.(RGAOpValue).V.Timestamp.(communication.VClock).Equal(op2.Value.(RGAOpValue).V.Timestamp.(communication.VClock)) {
		log.Println("REPAIRCAUSAL", op1, op2)
		return communication.Operation{
			Type:    op2.Type,
			Version: op2.Version,
			Value: RGAOpValue{
				Vertex{
					Timestamp: communication.VClock{},
				},
				op2.Value.(RGAOpValue).Value,
			},
			OriginID: op2.OriginID,
		}
	}
	return op2
}

func (r RGA) MainOp() string {
	return "Add"
}

// initialize RGA
func NewRGAReplica(id string, channels map[string]chan any, delay int) *replica.Replica {
	r := crdt.NewSemidirect2CRDT(id, []Vertex{{communication.NewVClockFromMap(map[string]uint64{}), "", id}}, RGA{id})

	return replica.NewReplica(id, r, channels, delay)
}

func indexOfVPtr(vertex Vertex, vertices []Vertex) int {
	for i, v := range vertices {
		if vertex.Timestamp.(communication.VClock).Equal(v.Timestamp.(communication.VClock)) {
			return i
		}
	}
	return -1
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

// deep copy state of RGA
func RGACopy(state []Vertex) []Vertex {
	stCpy := make([]Vertex, len(state))
	for i, v := range state {
		stCpy[i].Value = v.Value
		stCpy[i].Timestamp = v.Timestamp.(communication.VClock).Copy()
		stCpy[i].OriginID = v.OriginID
	}
	return stCpy
}
