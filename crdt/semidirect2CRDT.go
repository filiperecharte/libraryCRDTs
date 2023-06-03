package crdt

import (
	"library/packages/communication"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
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

	// Repairs unstable operations.
	Repair(op1 communication.Operation, op2 communication.Operation) communication.Operation
}

type Semidirect2CRDT struct {
	Id                  string
	Data                Semidirect2DataI                             //data interface
	Unstable_operations graph.Graph[string, communication.Operation] //all aplied updates
	Unstable_st         any
	N_Ops               uint64

	effectLock *sync.RWMutex
}

// initialize semidirectcrdt
func NewSemidirect2CRDT(id string, state any, data Semidirect2DataI) *Semidirect2CRDT {
	c := Semidirect2CRDT{Id: id,
		Data:                data,
		Unstable_operations: graph.New(opHash, graph.Directed(), graph.Acyclic()),
		Unstable_st:         state,
		N_Ops:               0,
		effectLock:          new(sync.RWMutex),
	}

	return &c
}

func (r *Semidirect2CRDT) Effect(op communication.Operation) {
	r.effectLock.Lock()
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

	file, _ := os.Create("./my" + r.Id + "graph.gv")
	_ = draw.DOT(r.Unstable_operations, file)

	r.N_Ops++
	r.effectLock.Unlock()
}

func (r *Semidirect2CRDT) Stabilize(op communication.Operation) {
	// for i, o := range r.Unstable_operations {
	// 	if o.Equals(op) {
	// 		r.Unstable_operations = append(r.Unstable_operations[:i], r.Unstable_operations[i+1:]...)
	// 		break
	// 	}
	// }
}

func (r *Semidirect2CRDT) Query() any {
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
