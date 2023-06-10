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

	// Repairs unstable operations.
	RepairCausal(op1 communication.Operation, op2 communication.Operation) communication.Operation
}

type NonMainOp struct {
	Op              communication.Operation
	HigherTimestamp communication.VClock //to stabilize Op, all operations with lower timestamp must be stable when Op is applied to the state
}

type Semidirect2CRDT struct {
	Id                  string
	Data                Semidirect2DataI                             //data interface
	Unstable_operations graph.Graph[string, communication.Operation] //all aplied updates
	Stable_operations   []communication.Operation
	NonMain_operations  []NonMainOp
	Unstable_st         any
	N_Ops               uint64

	higherTimestamp communication.VClock

	effectLock *sync.RWMutex
}

// initialize semidirectcrdt
func NewSemidirect2CRDT(id string, state any, data Semidirect2DataI) *Semidirect2CRDT {
	c := Semidirect2CRDT{
		Id:                  id,
		Data:                data,
		Unstable_operations: graph.New(opHash2, graph.Directed(), graph.Acyclic()),
		NonMain_operations:  []NonMainOp{},
		Unstable_st:         state,
		N_Ops:               0,
		effectLock:          new(sync.RWMutex),

		higherTimestamp: communication.VClock{},
	}

	return &c
}

func (r *Semidirect2CRDT) Effect(op communication.Operation) {
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	r.higherTimestamp = op.Version

	if r.Data.MainOp() != op.Type {
		r.NonMain_operations = append(r.NonMain_operations, NonMainOp{op, communication.VClock{}})
		r.N_Ops++
		return
	}

	t, err := graph.StableTopologicalSort(r.Unstable_operations, less)
	if err != nil {
		panic(err)
	}

	op = r.repairCausal(op)

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
			r.Unstable_operations.AddEdge(opHash2(vertex), opHash2(op))
		} else if !commutative && !ordered {
			r.Unstable_operations.AddEdge(opHash2(op), opHash2(vertex))
		}
	}

	r.N_Ops++
}

func (r *Semidirect2CRDT) Stabilize(op communication.Operation) {
	r.effectLock.Lock()
	defer r.effectLock.Unlock()

	for i, v := range r.NonMain_operations {
		if v.HigherTimestamp.Equal(r.higherTimestamp) {
			r.NonMain_operations = append(r.NonMain_operations[:i], r.NonMain_operations[i+1:]...)
			break
		}
	}

	if r.Data.MainOp() != op.Type {
		//remove from non main operations
		for i, v := range r.NonMain_operations {
			if v.Op.Equals(op) {
				//r.NonMain_operations = append(r.NonMain_operations[:i], r.NonMain_operations[i+1:]...)
				r.NonMain_operations[i].HigherTimestamp = r.higherTimestamp
				r.Unstable_st = r.Data.Apply(r.Unstable_st, []communication.Operation{op})
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
			if edge.Source == opHash2(op) || edge.Target == opHash2(op) {
				r.Unstable_operations.RemoveEdge(edge.Source, edge.Target)
			}
		}
	}

	r.Unstable_operations.RemoveVertex(opHash2(op))
}

func (r *Semidirect2CRDT) Query() (any, any) {
	//apply all non main operations
	nonMainOp := r.getNonMainOperations()
	query_st := r.Data.Apply(r.Unstable_st, nonMainOp)
	return query_st, nonMainOp
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

func (r *Semidirect2CRDT) repairCausal(op communication.Operation) communication.Operation {
	for _, nonOP := range r.NonMain_operations {
		if nonOP.Op.Version.Compare(op.Version) == communication.Descendant {
			op = r.Data.RepairCausal(nonOP.Op, op)
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

func opHash2(op communication.Operation) string {
	return strconv.FormatUint(op.Version.Sum(), 10) + op.OriginID
}

func (r Semidirect2CRDT) getNonMainOperations() []communication.Operation {
	nonMainOps := []communication.Operation{}
	for _, op := range r.NonMain_operations {
		if op.Op.Type != "Add" {
			nonMainOps = append(nonMainOps, op.Op)
		}
	}
	return nonMainOps
}
