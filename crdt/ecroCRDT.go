package crdt

import (
	"library/packages/communication"
	"strconv"
	"sync"

	"github.com/dominikbraun/graph"
)

// data interface
type EcroDataI interface {
	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// Order unstable operations.
	Order(op1 communication.Operation, op2 communication.Operation) bool

	//Operations that commute
	Commutes(op1 communication.Operation, op2 communication.Operation) bool
}

type EcroCRDT struct {
	Id                  string
	Data                EcroDataI //data interface
	Stable_st           any       // stable state
	Unstable_operations graph.Graph[string, communication.Operation]
	Stable_operation    communication.Operation
	Unstable_st         any //most recent state
	N_Ops               uint64

	StabilizeLock *sync.RWMutex
}

// initialize ecrocrdt
func NewEcroCRDT(id string, state any, data EcroDataI) *EcroCRDT {
	c := EcroCRDT{Id: id,
		Data:                data,
		Stable_st:           state,
		Unstable_operations: graph.New(opHash, graph.Directed(), graph.Acyclic()),
		Unstable_st:         state,
		N_Ops:               0,
		StabilizeLock:       new(sync.RWMutex),
	}

	return &c
}

func (r *EcroCRDT) Effect(op communication.Operation) {
	r.StabilizeLock.Lock()
	r.Unstable_operations.AddVertex(op, graph.VertexAttribute("label", opHash(op)+" "+op.Type+" "+op.Version.ReturnVCString()))
	if r.addEdges(op) {
		r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{op})
	} else {
		r.Unstable_st = r.Data.Apply(r.Stable_st, r.topologicalSort())
	}

	r.StabilizeLock.Unlock()
	r.N_Ops++
}

func (r *EcroCRDT) Stabilize(op communication.Operation) {
	r.StabilizeLock.Lock()
	defer r.StabilizeLock.Unlock()
	//remove vertex of the operation and all its edges
	r.Stable_operation = op
	t := r.topologicalSort()
	io := indexOf(t, op)

	if !r.prefixStable(t, io) {
		return
	}

	//remove all edges that have the operation as target or source
	adjacencyMap, _ := r.Unstable_operations.AdjacencyMap()
	for _, edges := range adjacencyMap {
		for _, edge := range edges {
			if edge.Source == opHash(op) || edge.Target == opHash(op) {
				r.Unstable_operations.RemoveEdge(edge.Source, edge.Target)
			}
		}
	}

	r.Unstable_operations.RemoveVertex(opHash(op))

	r.Stable_st = r.Data.Apply(r.Stable_st, t[:io+1])
	//r.Unstable_st = r.Data.Apply(r.Stable_st, t[io+1:])
}

func (r *EcroCRDT) Query() (any, any) {
	r.StabilizeLock.Lock()
	defer r.StabilizeLock.Unlock()
	return r.Unstable_st, nil
}

func (r *EcroCRDT) NumOps() uint64 {
	return r.N_Ops
}

// add edges to graph and return if its descendant of all operations or not
func (r *EcroCRDT) addEdges(op communication.Operation) bool {
	isDescendant := true
	adjacencyMap, _ := r.Unstable_operations.AdjacencyMap()
	for vertexHash := range adjacencyMap {
		vertex, _ := r.Unstable_operations.Vertex(vertexHash)
		if op.Equals(vertex) {
			continue
		}
		cmp := op.Version.Compare(vertex.Version)
		opHash := opHash(op)

		if cmp == communication.Ancestor && !r.Data.Commutes(op, vertex) {
			r.Unstable_operations.AddEdge(vertexHash, opHash, graph.EdgeAttributes(map[string]string{"label": "hb", "id": vertexHash + opHash}))
		} else if cmp == communication.Concurrent && !r.Data.Commutes(op, vertex) {
			isDescendant = false
			if r.Data.Order(op, vertex) {
				r.Unstable_operations.AddEdge(opHash, vertexHash, graph.EdgeAttributes(map[string]string{"label": "ao", "id": opHash + vertexHash}))
			} else if r.Data.Order(vertex, op) {
				r.Unstable_operations.AddEdge(vertexHash, opHash, graph.EdgeAttributes(map[string]string{"label": "ao", "id": vertexHash + opHash}))
			}
		}
	}

	return isDescendant
}

// creates hash for operation
func opHash(op communication.Operation) string {
	return op.OriginID + strconv.FormatUint(op.Version.Sum(), 10)
}

// orders the operations in the graph
func (r EcroCRDT) topologicalSort() []communication.Operation {
	//find minimum vertex of the graph (vertex with no incoming edges)
	//it can have more than one minimum, choose deterministically (by finding the minimum id) and continue algorithm

	//if the minimum exists put it in the topological order and search for the next recursively

	//if the minimum does not exist, the graph has cycles
	//the algorithm kills an arbitration edge deterministically (by finding the edge with the minimum id)
	//after killing the edge one of the verices will be the minimum if there is only one cycle
	//if there's another cycle repeat the process

	var order []communication.Operation
	removedVertices := make(map[string]bool)
	removedEdges := make(map[string]map[string]bool)

	for {
		// Create map to count incoming edges
		inDegree := make(map[string]int)
		g, _ := r.Unstable_operations.AdjacencyMap()

		for vertex := range g {
			inDegree[vertex] = 0 // Initialize inDegree for all vertices to 0
		}

		for _, edges := range g {
			for _, edge := range edges {
				if !removedVertices[edge.Source] && !removedVertices[edge.Target] && !removedEdges[edge.Source][edge.Target] {
					inDegree[edge.Target]++
				}
			}
		}

		// Find minimum vertex
		minVertex := communication.Operation{Type: ""}
		for vertex, degree := range inDegree {
			if degree == 0 && !removedVertices[vertex] {
				if minVertex.Type == "" || vertex < opHash(minVertex) {
					minVertex, _ = r.Unstable_operations.Vertex(vertex)
				}
			}
		}

		// If no minimum vertex found, there is a cycle
		if minVertex.Type == "" {
			minEdge := graph.Edge[string]{Source: "", Target: "", Properties: graph.EdgeProperties{Attributes: map[string]string{"label": "ao"}}}
			for _, edges := range g {
				for _, edge := range edges {
					if edge.Properties.Attributes["label"] == "ao" && (minEdge.Source == "" || edge.Properties.Attributes["id"] < minEdge.Properties.Attributes["id"]) && !removedEdges[edge.Source][edge.Target] {
						minEdge = edge
					}
				}
			}

			// Remove the minimum ID edge from the graph
			if removedEdges[minEdge.Source] == nil {
				removedEdges[minEdge.Source] = make(map[string]bool)
			}
			removedEdges[minEdge.Source][minEdge.Target] = true
			continue
		}

		// Add minimum vertex to topological order and "remove" it from the graph
		order = append(order, minVertex)
		removedVertices[opHash(minVertex)] = true

		// If all vertices are "removed", we are done
		if len(order) == len(g) {
			break
		}
	}

	return order
}

// gets index of operation in array
func indexOf(operations []communication.Operation, op communication.Operation) int {
	for i, o := range operations {
		if op.Equals(o) {
			return i
		}
	}
	return -1
}

// check if prefix of the operations is stable (all operations of the prefix are in stable_operations)
func (r EcroCRDT) prefixStable(operations []communication.Operation, index int) bool {
	for _, o := range operations[:index+1] {
		if r.Stable_operation.Version.Compare(o.Version) != communication.Descendant {
			return false
		}
	}
	return true
}
