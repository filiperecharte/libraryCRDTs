package crdt

import (
	"library/packages/communication"
	"strconv"
	"sync"

	"github.com/dominikbraun/graph"
)

// all updates are reparable
type Semidirect2DataI interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state any, operations []communication.Operation) any

	// ArbitrationOrder returns two booleans
	// the first tells if the op2 is repairable knowing op1
	// the second tells if the order op1 > op2 is correct or needs to be swapped
	ArbitrationOrder(op1 communication.Operation, op2 communication.Operation) (bool, bool)

	MainOp() string

	// Repairs unstable operations.
	Repair(op1 communication.Operation, op2 communication.Operation) communication.Operation
}

type Semidirect2CRDT struct {
	Id                  string
	Data                Semidirect2DataI                             //data interface
	Unstable_operations graph.Graph[string, communication.Operation] //all aplied updates
	Stable_operations   []communication.Operation
	NonMain_operations  []communication.Operation
	Unstable_st         any
	N_Ops               uint64

	effectLock *sync.RWMutex
}

// initialize semidirectcrdt
func NewSemidirect2CRDT(id string, state any, data Semidirect2DataI) *Semidirect2CRDT {
	c := Semidirect2CRDT{
		Id:                  id,
		Data:                data,
		Unstable_operations: graph.New(opHash, graph.Directed(), graph.Acyclic()),
		NonMain_operations:  []communication.Operation{},
		Unstable_st:         state,
		N_Ops:               0,
		effectLock:          new(sync.RWMutex),
	}

	return &c
}

func (r *Semidirect2CRDT) Effect(op communication.Operation) {
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	if r.Data.MainOp() != op.Type {
		r.NonMain_operations = append(r.NonMain_operations, op)
		r.N_Ops++
		return
	}

	t, err := graph.StableTopologicalSort(r.Unstable_operations, less)
	if err != nil {
		panic(err)
	}
	newOp := r.repair(op, t)
	r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{newOp})

	//Add vertex
	r.Unstable_operations.AddVertex(op)

	//insert operation on log
	adjancecyMap, _ := r.Unstable_operations.AdjacencyMap()
	for vertexHash, _ := range adjancecyMap {
		vertex, _ := r.Unstable_operations.Vertex(vertexHash)
		if op.Equals(vertex) {
			continue
		}
		if commutative, ordered := r.Data.ArbitrationOrder(vertex, op); !commutative && ordered {
			r.Unstable_operations.AddEdge(opHash(vertex), opHash(op))
		} else if !commutative && !ordered {
			r.Unstable_operations.AddEdge(opHash(op), opHash(vertex))
		}
	}

	r.N_Ops++
}

func (r *Semidirect2CRDT) Stabilize(op communication.Operation) {
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	if r.Data.MainOp() != op.Type {
		//remove from non main operations
		for i, v := range r.NonMain_operations {
			if v.Equals(op) {
				r.NonMain_operations = append(r.NonMain_operations[:i], r.NonMain_operations[i+1:]...)
				break
			}
		}
		return
	}

	//remove vertex of the operation and all its edges
	r.Stable_operations = append(r.Stable_operations, op)
	t, err := graph.StableTopologicalSort(r.Unstable_operations, less)
	if err != nil {
		panic(err)
	}
	io := r.indexOf(t, op)

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
}

func (r *Semidirect2CRDT) Query() any {
	//apply all non main operations
	//query_st := r.Data.Apply(r.Unstable_st, r.NonMain_operations)
	return r.Unstable_st
}

func (r *Semidirect2CRDT) NumOps() uint64 {
	return r.N_Ops
}

func (r *Semidirect2CRDT) repair(op communication.Operation, ops []string) communication.Operation {

	//find operations that is concurrent with op
	for _, v := range ops {
		o, _ := r.Unstable_operations.Vertex(v)
		if o.Version.Compare(op.Version) == communication.Concurrent {
			op = r.Data.Repair(o, op)
		}
	}

	return op
}

func less(a, b string) bool {
	n1, _ := strconv.Atoi(a)
	n2, _ := strconv.Atoi(b)
	return n1 < n2
}

// check if prefix of the operations is stable (all operations of the prefix are in stable_operations)
func (r Semidirect2CRDT) prefixStable(operations []string, index int) bool {
	for _, vertexHash := range operations[:index+1] {
		o, _ := r.Unstable_operations.Vertex(vertexHash)
		if !contains(r.Stable_operations, o) {
			return false
		}
	}
	return true
}

// gets index of operation in array
func (r Semidirect2CRDT) indexOf(operations []string, op communication.Operation) int {
	for i, vertexHash := range operations {
		o, _ := r.Unstable_operations.Vertex(vertexHash)
		if op.Equals(o) {
			return i
		}
	}
	return -1
}
