package crdt

import (
	"library/packages/communication"
	"library/packages/replica"
	"log"
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
	Sorted_ops          []string
	discarded           map[string]graph.Edge[string]
	Replica             replica.Replica

	N_Ops uint64
	S_Ops uint64

	StabilizeLock *sync.RWMutex
}

// initialize ecrocrdt
func NewEcroCRDT(id string, state any, data EcroDataI, replica replica.Replica) *EcroCRDT {
	c := EcroCRDT{Id: id,
		Data:                data,
		Stable_st:           state,
		Unstable_operations: graph.New(opHash, graph.Directed(), graph.PreventCycles()),
		Unstable_st:         state,
		N_Ops:               0,
		S_Ops:               0,
		StabilizeLock:       new(sync.RWMutex),
		discarded:           map[string]graph.Edge[string]{},
		Replica:             replica,
	}

	return &c
}

func (r *EcroCRDT) SetReplica(rep *replica.Replica) {
	r.Replica = *rep
}

func (r *EcroCRDT) Effect(op communication.Operation) {
	r.StabilizeLock.Lock()
	defer r.StabilizeLock.Unlock()

	r.Unstable_operations.AddVertex(op, graph.VertexAttribute("label", opHash(op)+" "+op.Type+" "+op.Version.ReturnVCString()))
	if r.addEdges(op) {
		r.Sorted_ops = append(r.Sorted_ops, opHash(op))
		r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{op})
	} else {
		r.Sorted_ops, _ = graph.StableTopologicalSort(r.Unstable_operations, r.sortByVertexID)

		op1, _ := r.Unstable_operations.Vertex(r.Sorted_ops[0])
		r.Unstable_st = r.Data.Apply(r.Stable_st, []communication.Operation{op1})

		for _, vertexHash := range r.Sorted_ops[1:] {
			opx, _ := r.Unstable_operations.Vertex(vertexHash)
			r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{opx})
		}
	}
	r.N_Ops++

	log.Println(r.Id, r.N_Ops)
}

func (r *EcroCRDT) Stabilize(op communication.Operation) {
	r.StabilizeLock.Lock()
	defer r.StabilizeLock.Unlock()

	r.S_Ops++

	//remove vertex of the operation and all its edges
	r.Stable_operation = op
	io := r.indexOf(r.Sorted_ops, op)

	if !r.prefixStable(r.Sorted_ops, io) {
		return
	}

	//remove all edges that have the operation as target or source
	adjacencyMap, _ := r.Unstable_operations.AdjacencyMap()
	for _, edges := range adjacencyMap {
		for _, edge := range edges {
			if edge.Source == opHash(op) || edge.Target == opHash(op) {
				r.Unstable_operations.RemoveEdge(edge.Source, edge.Target)
				delete(r.discarded, edge.Properties.Attributes["id"])
			}
		}
	}

	r.Unstable_operations.RemoveVertex(opHash(op))

	for _, vertexHash := range r.Sorted_ops[:io+1] {
		opx, _ := r.Unstable_operations.Vertex(vertexHash)
		r.Stable_st = r.Data.Apply(r.Stable_st, []communication.Operation{opx})
	}

	//remove operation from sorted ops
}

func (r *EcroCRDT) RemovedEdge(op communication.Operation) {
	r.StabilizeLock.Lock()
	defer r.StabilizeLock.Unlock()

	for key, edge := range op.Value.(map[string]graph.Edge[string]) {
		r.discarded[key] = edge
	}

	r.Sorted_ops, _ = graph.StableTopologicalSort(r.Unstable_operations, r.sortByVertexID)
}

func (r *EcroCRDT) Query() (any, any) {
	r.StabilizeLock.Lock()
	defer r.StabilizeLock.Unlock()

	return r.Unstable_st, nil
}

func (r *EcroCRDT) NumOps() uint64 {
	return r.N_Ops
}

func (r *EcroCRDT) NumSOps() uint64 {
	return r.S_Ops
}

// add edges to graph and return if its descendant of all operations or not
func (r *EcroCRDT) addEdges(op communication.Operation) bool {
	isSafe := true
	new_discarded := map[string]graph.Edge[string]{}

	for _, edge := range r.discarded {
		r.Unstable_operations.RemoveEdge(edge.Source, edge.Target)
	}

	adjacencyMap, _ := r.Unstable_operations.AdjacencyMap()
	for vertexHash := range adjacencyMap {
		vertex, _ := r.Unstable_operations.Vertex(vertexHash)
		if op.Equals(vertex) || r.Data.Commutes(op, vertex) {
			continue
		}
		cmp := op.Version.Compare(vertex.Version)
		opHash := opHash(op)
		if cmp == communication.Ancestor {
			err := r.Unstable_operations.AddEdge(vertexHash, opHash, graph.EdgeAttributes(map[string]string{"label": "hb", "id": vertexHash + opHash}))
			if err != nil {

				edges := r.resolveCycle(vertexHash, opHash)

				for _, edge := range edges {
					new_discarded[edge.Properties.Attributes["id"]] = edge
					r.Unstable_operations.RemoveEdge(edge.Source, edge.Target)
				}

				err := r.Unstable_operations.AddEdge(vertexHash, opHash, graph.EdgeAttributes(map[string]string{"label": "hb", "id": vertexHash + opHash}))
				if err != nil {
					panic(err)
				}
			}
		} else if cmp == communication.Concurrent {
			isSafe = false
			if r.Data.Order(op, vertex) {
				err := r.Unstable_operations.AddEdge(opHash, vertexHash, graph.EdgeAttributes(map[string]string{"label": "ao", "id": opHash + vertexHash}))
				if err != nil {
					new_discarded[opHash+vertexHash] = graph.Edge[string]{Source: opHash, Target: vertexHash}
				}
			} else if r.Data.Order(vertex, op) {
				err := r.Unstable_operations.AddEdge(vertexHash, opHash, graph.EdgeAttributes(map[string]string{"label": "ao", "id": vertexHash + opHash}))
				if err != nil {
					new_discarded[vertexHash+opHash] = graph.Edge[string]{Source: vertexHash, Target: opHash}
				}
			}
		}
	}

	if len(new_discarded) == 0 {
		return isSafe
	}

	for _, edge := range new_discarded {
		r.discarded[edge.Properties.Attributes["id"]] = edge
	}

	go r.Replica.PropagateDiscarededEdges(r.discarded)

	return isSafe
}

// creates hash for operation
func opHash(op communication.Operation) string {
	return strconv.FormatUint(op.Version.Sum(), 10) + op.OriginID
}

// orders the operations in the graph
func (r *EcroCRDT) resolveCycle(s, d string) []graph.Edge[string] {
	new_discarded := []graph.Edge[string]{}
	paths := r.allPaths(d, s)

	for _, path := range paths {
		minEdge := graph.Edge[string]{Source: path[0], Target: path[1], Properties: graph.EdgeProperties{Attributes: map[string]string{"label": "ao", "id": path[0] + path[1]}}}
		for i := 0; i < len(path)-2; i++ {
			edge, err := r.Unstable_operations.Edge(path[i], path[i+1])
			if err != nil && edge.Properties.Attributes["label"] == "ao" && edge.Properties.Attributes["id"] < minEdge.Properties.Attributes["id"] {
				minEdge = graph.Edge[string]{Source: path[i], Target: path[i+1], Properties: graph.EdgeProperties{Attributes: map[string]string{"label": "ao", "id": path[i] + path[i+1]}}}
			}
		}
		new_discarded = append(new_discarded, graph.Edge[string]{Source: minEdge.Source, Target: minEdge.Target})
	}
	return new_discarded
}

// sorts the vertices by their id
func (r *EcroCRDT) sortByVertexID(v1 string, v2 string) bool {
	return v1 < v2
}

// gets index of operation in array
func (r EcroCRDT) indexOf(operations []string, op communication.Operation) int {
	for i, o := range operations {
		top, _ := r.Unstable_operations.Vertex(o)
		if op.Equals(top) {
			return i
		}
	}
	return -1
}

// check if prefix of the operations is stable (all operations of the prefix are in stable_operations)
func (r EcroCRDT) prefixStable(operations []string, index int) bool {
	for _, o := range operations[:index+1] {
		top, _ := r.Unstable_operations.Vertex(o)
		if top.Version.Compare(r.Stable_operation.Version) != communication.Descendant {
			return false
		}
	}
	return true
}

func (r *EcroCRDT) allPathsUtil(u, d string, visited *map[string]bool, path *[]string, paths *[][]string) {
	(*visited)[u] = true
	*path = append(*path, u)

	if u == d {
		// Make a deep copy of path, since path is reused
		newPath := make([]string, len(*path))
		copy(newPath, *path)
		*paths = append(*paths, newPath)
	} else {
		adjacencyMap, _ := r.Unstable_operations.AdjacencyMap()
		for vertexHash := range adjacencyMap[u] {
			if !(*visited)[vertexHash] {
				r.allPathsUtil(vertexHash, d, visited, path, paths)
			}
		}
	}

	*path = (*path)[:len(*path)-1]
	(*visited)[u] = false
}

func (r *EcroCRDT) allPaths(s, d string) [][]string {
	nVertices, _ := r.Unstable_operations.Order()
	visited := make(map[string]bool, nVertices)
	path := make([]string, 0)
	paths := make([][]string, 0)

	r.allPathsUtil(s, d, &visited, &path, &paths)
	return paths
}
