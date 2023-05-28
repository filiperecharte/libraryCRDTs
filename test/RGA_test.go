package test

import (
	"library/packages/communication"
	datatypes "library/packages/datatypes/ecro"
	"library/packages/replica"
	"log"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"
)

// variable with the alphabet to generate random strings
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func TestRGA(t *testing.T) {

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
			replicas[i] = datatypes.NewRGAReplica(strconv.Itoa(i), channels, numOperations-operations[i])
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(1)

			go func(r *replica.Replica, operations int) {
				defer wg.Done()
				// Perform random number of add operations with random delays

				for j := 0; j < operations; j++ {
					//choose a predecessor or a vertex to remove randomly from query
					q := r.Crdt.Query().([]datatypes.Vertex)
					v := q[rand.Intn(len(q))]
					//create a new operation
					op := communication.Operation{}
					//choose random leter to add
					value := letters[rand.Intn(len(letters))]

					//choose randomly if it is an add or remove operation
					if rand.Intn(2) == 0 {
						op.Type = "Add"
					} else {
						op.Type = "Rem"
					}

					op.Value = datatypes.RGAOpValue{
						Value: value,
						V:     v,
					}

					r.Prepare(op.Type, op.Value)
				}

			}(replicas[i], operations[i])
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
			if !datatypes.RGAEqual(replicas[i].Crdt.Query().([]datatypes.Vertex), replicas[0].Crdt.Query().([]datatypes.Vertex)) {
				for i := 0; i < numReplicas; i++ {
					t.Error("Replica ", i)
					q := replicas[i].Crdt.Query().([]datatypes.Vertex)
					for j := 0; j < len(q); j++ {
						log.Println(q[j])
					}
				}
				return false
			}
		}
		for i := 0; i < numReplicas; i++ {
			q := replicas[i].Crdt.Query().([]datatypes.Vertex)
			for j := 0; j < len(q); j++ {
				log.Println(q[j])
			}
		}


		log.Println("TEST PASSED")
		return true
	}

	// Define generator to limit input size
	gen := func(vals []reflect.Value, rand *rand.Rand) {
		operations_rep0 := 10
		operations_rep1 := 10
		operations_rep2 := 10

		operations := []int{operations_rep0, operations_rep1, operations_rep2}
		vals[0] = reflect.ValueOf(operations)      //operations
		vals[1] = reflect.ValueOf(len(operations)) //number of replicas
		vals[2] = reflect.ValueOf(30)              //number of operations
	}

	// Define config for quick.Check
	config := &quick.Config{
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		MaxCount: 100,
		Values:   gen,
	}

	// Generate and test random inputs
	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}
