package datatypes

import (
	"library/packages/communication"
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
