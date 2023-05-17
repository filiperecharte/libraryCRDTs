package utils

import "library/packages/communication"

// Graph represents a set of vertices connected by edges.
type Graph struct {
	Vertices map[int]*Vertex
}

// Vertex is a node in the graph that stores the int value at that node
// along with a map to the vertices it is connected to via edges.
type Vertex struct {
	Operation communication.Operation
	Edges     map[int]*Edge
}

// Edge represents an edge in the graph and the destination vertex.
type Edge struct {
	Vertex *Vertex
}

func (this *Graph) AddVertex(key int, op communication.Operation) {
	this.Vertices[key] = &Vertex{Operation: op, Edges: map[int]*Edge{}}
}

func (this *Graph) AddEdge(srcKey, destKey int) {
	// check if src & dest exist
	if _, ok := this.Vertices[srcKey]; !ok {
		return
	}
	if _, ok := this.Vertices[destKey]; !ok {
		return
	}

	// add edge src --> dest
	this.Vertices[srcKey].Edges[destKey] = &Edge{Vertex: this.Vertices[destKey]}
}

func (this *Graph) Neighbors(srcKey int) []communication.Operation {
	result := []communication.Operation{}

	for _, edge := range this.Vertices[srcKey].Edges {
		result = append(result, edge.Vertex.Operation)
	}

	return result
}
