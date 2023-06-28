package crdt

import (
	"library/packages/communication"
	"strconv"
	"sync"

	"library/packages/utils"

	"github.com/dominikbraun/graph"
)

// all updates are reparable
type SemidirectECRODataI interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// ArbitrationOrder returns two booleans
	// the first tells if the op2 is repairable knowing op1
	// the second tells if the order op1 > op2 is correct or needs to be swapped
	ArbitrationOrderMain(op1 communication.Operation, op2 communication.Operation) (bool, bool)

	Commutes(op1 communication.Operation, op2 communication.Operation) bool

	Order(op1 communication.Operation, op2 communication.Operation) bool

	SemidirectOps() []string

	ECROOps() []string

	// Repairs unstable operations.
	RepairRight(op1 communication.Operation, op2 communication.Operation, state any) communication.Operation

	// Repairs unstable operations.
	RepairLeft(op1 communication.Operation, op2 communication.Operation) communication.Operation
}

type ECROOp struct {
	Op              communication.Operation
	HigherTimestamp []communication.Operation //to stabilize Op, all operations with lower timestamp must be stable when Op is applied to the state
}

type SemidirectECRO struct {
	Id                   string
	Data                 SemidirectECRODataI //data interface
	Stable_st            any
	SemidirectLog        []communication.Operation
	StableMain_operation communication.Operation
	ECROLog              graph.Graph[string, ECROOp]
	Unstable_st          any
	Sorted_ops           []communication.Operation

	N_Ops uint64
	S_Ops uint64

	effectLock *sync.RWMutex
}

// initialize semidirectcrdt
func NewSemidirectECRO(id string, state any, data SemidirectECRODataI) *SemidirectECRO {
	c := SemidirectECRO{
		Id:            id,
		Data:          data,
		Stable_st:     state,
		SemidirectLog: []communication.Operation{},
		ECROLog:       graph.New(opHashSemiECRO, graph.Directed(), graph.Acyclic()),
		Unstable_st:   state,
		Sorted_ops:    []communication.Operation{},
		N_Ops:         0,
		S_Ops:         0,
		effectLock:    new(sync.RWMutex),
	}

	return &c
}

func (r *SemidirectECRO) Effect(op communication.Operation) {
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	r.N_Ops++

	//------------------------- ECRO ------------------------

	if utils.ContainsString(r.Data.ECROOps(), op.Type) {
		ecroOp := ECROOp{op, []communication.Operation{}}
		r.ECROLog.AddVertex(ecroOp, graph.VertexAttribute("label", opHashSemiECRO(ecroOp)+" "+op.Type+" "+op.Version.ReturnVCString()))

		//checks if op respects arbitration order
		if r.addEdges(op) {
			r.Sorted_ops = append(r.Sorted_ops, op)
			r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{op})
		} else {
			r.Sorted_ops = r.incTopologicalSort(r.Sorted_ops, op)
			r.Unstable_st = r.Data.Apply(r.Stable_st, r.Sorted_ops)
		}

		return
	}

	ecroNewOP := r.repairRight(op)

	op = r.repairLeft(op)

	//-------------------------------------------------------

	// --------------- semidirect continuous ----------------
	newOp := r.repairRight(op)

	r.Stable_st = r.Data.Apply(r.Stable_st, []communication.Operation{newOp})

	//add repairLeft operation to log
	//iterate starting from the end over unstable operations to find the correct position to insert the new operation
	if len(r.SemidirectLog) == 0 {
		r.SemidirectLog = append(r.SemidirectLog, op)
	} else {
		inserted := false
		for i := len(r.SemidirectLog) - 1; i >= 0; i-- {
			//if it respects arbitration order, insert it
			if _, ok := r.Data.ArbitrationOrderMain(r.SemidirectLog[i], op); ok {
				r.SemidirectLog = append(r.SemidirectLog[:i+1], append([]communication.Operation{op}, r.SemidirectLog[i+1:]...)...)
				inserted = true
				break
			}
		}
		if !inserted {
			r.SemidirectLog = append([]communication.Operation{op}, r.SemidirectLog...)
		}
	}
	//-------------------------------------------------------

	if r.hasConcurrentRem(ecroNewOP) {
		//add operation to unstable state
		r.Sorted_ops = append(r.Sorted_ops, ecroNewOP)
		r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{ecroNewOP})
	} else {
		r.Sorted_ops = r.incTopologicalSort(r.Sorted_ops, op)
		r.Unstable_st = r.Data.Apply(r.Stable_st, r.Sorted_ops)
	}

}

func (r *SemidirectECRO) hasConcurrentRem(op communication.Operation) bool {
	//checks if op respects arbitration order
	adjacencyMap, _ := r.ECROLog.AdjacencyMap()
	for vertexHash := range adjacencyMap {
		vertex, _ := r.ECROLog.Vertex(vertexHash)
		if vertex.Op.Version.Compare(op.Version) != communication.Concurrent || !r.Data.Commutes(vertex.Op, op) {
			return false
		}
	}
	return true
}

func (r *SemidirectECRO) Stabilize(op communication.Operation) {
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	r.S_Ops++

	if utils.ContainsString(r.Data.SemidirectOps(), op.Type) {
		r.StableMain_operation = op
	}

	adjacencyMap, _ := r.ECROLog.AdjacencyMap()
	for vertexHash := range adjacencyMap {
		vertex, _ := r.ECROLog.Vertex(vertexHash)
		if r.becameStable(vertex.HigherTimestamp) {

			//remove all edges that have the operation as target or source
			adjacencyMap, _ := r.ECROLog.AdjacencyMap()
			for _, edges := range adjacencyMap {
				for _, edge := range edges {
					if edge.Source == vertexHash || edge.Target == vertexHash {
						r.ECROLog.RemoveEdge(edge.Source, edge.Target)
					}
				}
			}

			//remove vertex of the operation
			r.ECROLog.RemoveVertex(vertexHash)
			break
		}
	}

	if utils.ContainsString(r.Data.ECROOps(), op.Type) {
		//remove from non main operations
		adjacencyMap, _ := r.ECROLog.AdjacencyMap()
		for vertexHash := range adjacencyMap {
			vertex, _ := r.ECROLog.Vertex(vertexHash)
			if vertex.Op.Equals(op) {
				newVertex := ECROOp{vertex.Op, r.getGreatestOps()}
				//r.NonMain_operations = append(r.NonMain_operations[:i], r.NonMain_operations[i+1:]...)
				r.updateVertex(vertex, newVertex)
				r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{op})
				break
			}
		}
		return
	}

	io := r.indexOf(op)

	if !r.prefixStable(io) {
		return
	}

	//remove operation from unstable operations
	r.SemidirectLog = append(r.SemidirectLog[:io], r.SemidirectLog[io+1:]...)
}

func (r *SemidirectECRO) Query() (any, any) {
	//apply all non main operations
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	nonMainOp := r.getNonMainOperations()
	return r.Unstable_st, nonMainOp
}

func (r *SemidirectECRO) NumOps() uint64 {
	return r.N_Ops
}

func (r *SemidirectECRO) NumSOps() uint64 {
	return r.N_Ops
}

func (r *SemidirectECRO) repairRight(op communication.Operation) communication.Operation {
	//find operations that is concurrent with op

	for _, o := range r.SemidirectLog {
		if o.Version.Compare(op.Version) == communication.Concurrent {
			op = r.Data.RepairRight(o, op, r.Stable_st)
		}
	}

	return op
}

func (r *SemidirectECRO) repairLeft(op communication.Operation) communication.Operation {
	adjacencyMap, _ := r.ECROLog.AdjacencyMap()
	for vertexHash := range adjacencyMap {
		vertex, _ := r.ECROLog.Vertex(vertexHash)
		if vertex.Op.Version.Compare(op.Version) == communication.Descendant {
			op = r.Data.RepairLeft(vertex.Op, op)
		}
	}

	return op
}

// check if prefix of the operations is stable (all operations of the prefix are in stable_operations)
func (r SemidirectECRO) prefixStable(index int) bool {
	if index == -1 {
		return false
	}
	for _, o := range r.SemidirectLog[:index+1] {
		if o.Version.Compare(r.StableMain_operation.Version) != communication.Descendant {
			return false
		}
	}
	return true
}

// gets index of operation in array
func (r SemidirectECRO) indexOf(op communication.Operation) int {
	for i, o := range r.SemidirectLog {
		if op.Equals(o) {
			return i
		}
	}
	return -1
}

func (r SemidirectECRO) getNonMainOperations() []communication.Operation {
	nonMainOps := []communication.Operation{}
	adjacencyMap, _ := r.ECROLog.AdjacencyMap()
	for vertexHash := range adjacencyMap {
		op, _ := r.ECROLog.Vertex(vertexHash)
		nonMainOps = append(nonMainOps, op.Op)
	}
	return nonMainOps
}

func (r SemidirectECRO) getGreatestOps() []communication.Operation {

	if len(r.SemidirectLog) == 0 {
		return []communication.Operation{}
	}

	//get greatest operations
	greatestOps := []communication.Operation{}
	greatestOp := r.SemidirectLog[len(r.SemidirectLog)-1]
	greatestOps = append(greatestOps, greatestOp)

	for i := len(r.SemidirectLog) - 2; i >= 0; i-- {
		//if its concurrent, add it to the list
		if r.SemidirectLog[i].Version.Compare(greatestOp.Version) == communication.Concurrent {
			greatestOps = append(greatestOps, r.SemidirectLog[i])
		}
	}

	return greatestOps
}

func (r SemidirectECRO) becameStable(ops []communication.Operation) bool {
	if len(ops) == 0 {
		return false
	}

	for _, op := range ops {
		if op.Version.Compare(r.StableMain_operation.Version) != communication.Descendant {
			return false
		}
	}
	return true
}

func (r *SemidirectECRO) incTopologicalSort(topoSort []communication.Operation, u communication.Operation) []communication.Operation {
	if len(topoSort) == 0 {
		return []communication.Operation{u}
	}

	x := topoSort[0]

	if x.Version.Compare(u.Version) == communication.Descendant {
		return append([]communication.Operation{x}, r.incTopologicalSort(topoSort[1:], u)...)

	} else if r.Data.Order(x, u) {
		isCausalDesc := false
		for _, y := range topoSort {
			if y.Version.Compare(x.Version) == communication.Descendant {
				isCausalDesc = true
			}
		}
		if !isCausalDesc {
			return append([]communication.Operation{x}, r.incTopologicalSort(topoSort[1:], u)...)
		}
	} else {
		isLess := true
		for _, y := range topoSort {
			if !(r.Data.Order(u, y)) {
				isLess = false
			}
		}
		if isLess {
			return append([]communication.Operation{u}, topoSort...)
		}
	}

	return r.topologicalSort(append([]communication.Operation{u}, topoSort...))
}

// orders the operations in the graph
func (r SemidirectECRO) topologicalSort(vertices []communication.Operation) []communication.Operation {
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
	//predecessorMap, _ := r.Unstable_operations.PredecessorMap()
	edgesG, _ := r.ECROLog.Edges()

	edges := []graph.Edge[string]{}

	for _, edge := range edgesG {
		target, _ := r.ECROLog.Vertex(edge.Target)
		source, _ := r.ECROLog.Vertex(edge.Source)
		if utils.Contains(vertices, target.Op) && utils.Contains(vertices, source.Op) {
			edges = append(edges, edge)
		}
	}

	for {
		// Create map to count incoming edges
		inDegree := make(map[string]int)

		for _, vertex := range vertices {
			inDegree[opHash(vertex)] = 0 // Initialize inDegree for all vertices to 0
		}

		for _, edge := range edges {
			if !removedVertices[edge.Source] && !removedVertices[edge.Target] && !removedEdges[edge.Source][edge.Target] {
				inDegree[edge.Target]++
			}
		}

		// Find minimum vertex
		minVertex := communication.Operation{Type: ""}

		for vertex, degree := range inDegree {
			if degree == 0 && !removedVertices[vertex] {
				if minVertex.Type == "" || vertex < opHash(minVertex) {
					minV, _ := r.ECROLog.Vertex(vertex)
					minVertex = minV.Op
				}
			}
		}

		// If no minimum vertex found, there is a cycle
		if minVertex.Type == "" {
			minEdge := graph.Edge[string]{Source: "", Target: "", Properties: graph.EdgeProperties{Attributes: map[string]string{"label": "ao"}}}
			for _, edge := range edges {
				if edge.Properties.Attributes["label"] == "ao" && (minEdge.Source == "" || edge.Properties.Attributes["id"] < minEdge.Properties.Attributes["id"]) && !removedEdges[edge.Source][edge.Target] {
					minEdge = edge
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
		if len(order) == len(vertices) {
			break
		}
	}

	return order
}

// add edges to graph and return if its descendant of all operations or not
func (r *SemidirectECRO) addEdges(op communication.Operation) bool {
	isSafe := true
	adjacencyMap, _ := r.ECROLog.AdjacencyMap()
	for vertexHash := range adjacencyMap {
		vertex, _ := r.ECROLog.Vertex(vertexHash)
		if op.Equals(vertex.Op) {
			continue
		}
		cmp := op.Version.Compare(vertex.Op.Version)
		opHash := opHash(op)

		if cmp == communication.Ancestor && !r.Data.Commutes(op, vertex.Op) {
			r.ECROLog.AddEdge(vertexHash, opHash, graph.EdgeAttributes(map[string]string{"label": "hb", "id": vertexHash + opHash}))
		} else if cmp == communication.Concurrent && !r.Data.Commutes(op, vertex.Op) {
			if r.Data.Order(op, vertex.Op) {
				isSafe = false
				r.ECROLog.AddEdge(opHash, vertexHash, graph.EdgeAttributes(map[string]string{"label": "ao", "id": opHash + vertexHash}))
			} else if r.Data.Order(vertex.Op, op) {
				r.ECROLog.AddEdge(vertexHash, opHash, graph.EdgeAttributes(map[string]string{"label": "ao", "id": vertexHash + opHash}))
			}
		}
	}

	return isSafe
}

// creates hash for operation
func opHashSemiECRO(op ECROOp) string {
	return op.Op.OriginID + strconv.FormatUint(op.Op.Version.Sum(), 10)
}

// update vertex of the graph by removing all edges that have the operation as target or source and then removing the vertex and adding it again
func (r *SemidirectECRO) updateVertex(op ECROOp, newop ECROOp) {

	tempEdges := []graph.Edge[string]{} //edges to be removed
	adjacencyMap, _ := r.ECROLog.AdjacencyMap()
	for _, edges := range adjacencyMap {
		for _, edge := range edges {
			if edge.Source == opHashSemiECRO(op) || edge.Target == opHashSemiECRO(op) {
				tempEdges = append(tempEdges, edge)
				r.ECROLog.RemoveEdge(edge.Source, edge.Target)
			}
		}
	}
	//remove from graph
	r.ECROLog.RemoveVertex(opHashSemiECRO(op))
	//add to graph
	r.ECROLog.AddVertex(newop, graph.VertexAttribute("label", opHashSemiECRO(newop)+" "+newop.Op.Type+" "+newop.Op.Version.ReturnVCString()))
	//add edges again
	for _, edge := range tempEdges {
		r.ECROLog.AddEdge(edge.Source, edge.Target, graph.EdgeAttributes(map[string]string{"label": edge.Properties.Attributes["label"], "id": edge.Properties.Attributes["id"]}))
	}
}
