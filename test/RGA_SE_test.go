package test

import (
	"library/packages/communication"
	datatypes "library/packages/datatypes/semidirect"
	"library/packages/replica"
	"library/packages/utils"
	"log"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/dominikbraun/graph"
)

func TestSERGA(t *testing.T) {

	// Define property to test
	property := func(operations []int, numReplicas int, numOperations int) bool {

		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		for i := 0; i < numReplicas; i++ {
			replicas[i] = datatypes.NewRGAReplica(strconv.Itoa(i), channels, numOperations-operations[i], nil)
		}

		operationsGraph := graph.New(opHash, graph.Directed(), graph.Acyclic())

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(1)

			go func(r *replica.Replica, operations int, g *graph.Graph[string, communication.Operation]) {
				defer wg.Done()
				// Perform random number of add operations with random delays

				for j := 0; j < operations; j++ {
					//choose a predecessor or a vertex to remove randomly from query
					v := generateRandomVertex(*r)

					//choose random leter to add
					value := letters[rand.Intn(len(letters))]

					//choose randomly if it is an add or remove operation
					OPType := "Add"
					if rand.Intn(2) == 0 {
						OPType = "Add"
					} else {
						OPType = "Rem"
						if v.Value == "" { //do not generate removes to the head of the list
							j--
							continue
						}
					}

					OPValue := datatypes.RGAOpValue{
						Value: value,
						V:     v,
					}

					op := r.Prepare(OPType, OPValue)

					err := (*g).AddVertex(op)
					if err != nil {
						panic(err)
					}
					addOp(op, g)

					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				}

			}(replicas[i], operations[i], &operationsGraph)
		}

		// Wait for all goroutines to finish
		wg.Wait()

		// Wait for all replicas to receive all messages
		for {
			flag := 0
			for i := 0; i < numReplicas; i++ {
				if replicas[i].Crdt.NumOps() == uint64(numOperations) {
					flag += 1
				}
			}
			if flag == numReplicas {
				break
			}
		}

		//Check that all replicas have the same state
		for i := 1; i < numReplicas; i++ {
			st, _ := replicas[i].Crdt.Query()
			stt, _ := replicas[0].Crdt.Query()
			if !datatypes.RGAEqual(st.([]datatypes.Vertex), stt.([]datatypes.Vertex)) {
				for i := 0; i < numReplicas; i++ {
					t.Error("Replica ", i)
					q, _ := replicas[i].Crdt.Query()
					for j := 0; j < len(q.([]datatypes.Vertex)); j++ {
						log.Println(q.([]datatypes.Vertex)[j])
					}
				}
				return false
			}
		}
		log.Println("All replicas have the same state")

		//Check that the state is consistent with a sequential execution
		rga := datatypes.RGA{}
		allTPs := utils.GetAllTopologicalOrders(&operationsGraph)

		for _, tp := range allTPs {

			state := []datatypes.Vertex{
				{
					Timestamp: communication.NewVClockFromMap(map[string]uint64{}),
					Value:     "",
					OriginID:  "0",
				},
			}

			for _, vertex := range tp {
				//apply operations in topological order
				op, _ := operationsGraph.Vertex(vertex)
				state = rga.Apply(state, []communication.Operation{op}).([]datatypes.Vertex)
			}

			//check that the state is the same as the state of the replicas
			st, _ := replicas[0].Crdt.Query()
			if datatypes.RGAEqual(state, st.([]datatypes.Vertex)) {
				log.Println("State has a sequential execution")
				return true
			}
		}

		t.Error("STATE NOT EQUAL TO CRDT STATE")
		return false
	}

	// Define generator to limit input size
	gen := func(vals []reflect.Value, rand *rand.Rand) {
		operations_rep0 := 5
		operations_rep1 := 5
		operations_rep2 := 5

		operations := []int{operations_rep0, operations_rep1, operations_rep2}
		vals[0] = reflect.ValueOf(operations)      //number of operations for each replica
		vals[1] = reflect.ValueOf(len(operations)) //number of replicas
		vals[2] = reflect.ValueOf(15)              //number of operations
	}

	// Define config for quick.Check
	config := &quick.Config{
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		MaxCount: 1,
		Values:   gen,
	}

	// Generate and test random inputs
	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}

// add operations to graph
func addOp(op communication.Operation, graph *graph.Graph[string, communication.Operation]) {
	(*graph).AddVertex(op)

	adjacencyMap, _ := (*graph).AdjacencyMap()
	for vertexHash := range adjacencyMap {
		vertex, _ := (*graph).Vertex(vertexHash)
		if op.Equals(vertex) {
			continue
		}
		cmp := op.Version.Compare(vertex.Version)
		opHash := opHash(op)

		if cmp == communication.Ancestor {
			(*graph).AddEdge(vertexHash, opHash)
		}
	}
}

func opHash(op communication.Operation) string {
	return op.OriginID + strconv.FormatUint(op.Version.Sum(), 10)
}
