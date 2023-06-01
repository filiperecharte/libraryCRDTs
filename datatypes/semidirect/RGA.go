package datatypes

import (
	"library/packages/communication"
	"library/packages/crdt"
	"library/packages/replica"
	"strconv"

	mapset "github.com/deckarep/golang-set/v2"
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
	st := state.([]Vertex)
	for _, op := range operations {
		msg := op
		switch msg.Type {
		case "Add":
			newVertex := Vertex{msg.Version, msg.Value.(RGAOpValue).Value, msg.OriginID}
			newVertexPrev := msg.Value.(RGAOpValue).V

			// find index where predecessor vertex can be found
			predecessorIdx := indexOfVPtr(newVertexPrev, st)

			newVertices := append(st[:predecessorIdx+1], append([]Vertex{newVertex}, st[predecessorIdx+1:]...)...)

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

func (r RGA) ArbitrationOrder(op1 communication.Operation, op2 communication.Operation) (bool, bool) {

	repair := false
	//verifies if the two operations are inserts after the same Vertex, if yes order by operation id (timestamp - vectorclock) -> will need repair
	if op1.Value.(RGAOpValue).V.Timestamp.(communication.VClock).Equal(op2.Value.(RGAOpValue).V.Timestamp.(communication.VClock)) {
		//arbitration order by ids
		order := op1.OriginID+strconv.Itoa(int(op1.Version.Sum())) < op2.OriginID+strconv.Itoa(int(op2.Version.Sum()))
		if !order {
			repair = true
		}
		return repair, order
		//if the insert is not after the same vertex:
	} else {
		//check if one of them is the previous vertex of another, if yes order by causality,
		if op1.Value.(RGAOpValue).Value == op1.Value.(RGAOpValue).V.Value || op2.Value.(RGAOpValue).Value == op1.Value.(RGAOpValue).V.Value {
			return false, true
			//if no, they are commutative and we can order them by any rule (e.g. ids)
		} else {
			return false, op1.OriginID+strconv.Itoa(int(op1.Version.Sum())) < op2.OriginID+strconv.Itoa(int(op2.Version.Sum()))
		}
	}
}

func (r RGA) Repair(op1 communication.Operation, op2 communication.Operation) communication.Operation {

	return communication.Operation{}
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
	r := crdt.Semidirect2CRDT{Id: id, Data: RGA{id}, Unstable_operations: []communication.Operation{}, Unstable_st: mapset.NewSet[any](), N_Ops: 0}

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
