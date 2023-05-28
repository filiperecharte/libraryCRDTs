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

func TestRGA(t *testing.T) {

	// Define property to test
	property := func(operations [][]communication.Operation, numReplicas int, numOperations int) bool {

		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		for i := 0; i < numReplicas; i++ {
			replicas[i] = datatypes.NewRGAReplica(strconv.Itoa(i), channels, numOperations-len(operations[i]))
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(1)

			go func(r *replica.Replica, operations []communication.Operation) {
				defer wg.Done()
				// Perform random number of add operations with random delays

				for j := 0; j < len(operations); j++ {
					for len(r.Crdt.Query().([]datatypes.Vertex)) <= operations[j].Value.(datatypes.RGAOpIndex).Index {
						log.Println("Waiting for previous operation to be on the state")
						time.Sleep(1 * time.Second)
					}
					v := datatypes.Vertex{}
					if operations[j].Type == "Add" {
						v = datatypes.GetPrevVertex(operations[j].Value.(datatypes.RGAOpIndex).Index, r.Crdt.Query().([]datatypes.Vertex))
					} else if operations[j].Type == "Rem" {
						v = datatypes.GetVertexRemove(operations[j].Value.(datatypes.RGAOpIndex).Index, r.Crdt.Query().([]datatypes.Vertex))
					}

					// Wait until previous operation is on the state for testing purposes, in a normal execution the operation would have to be there
					operations[j].Value = datatypes.RGAOpValue{
						Value: operations[j].Value.(datatypes.RGAOpIndex).Value,
						V:     v,
					}

					r.Prepare(operations[j].Type, operations[j].Value)
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

		for i := 0; i < numReplicas; i++ {
			log.Println(replicas[i].Crdt.Query().([]datatypes.Vertex))
		}

		//Check that all replicas have the same state
		for i := 1; i < numReplicas; i++ {
			if !datatypes.RGAEqual(replicas[i].Crdt.Query().([]datatypes.Vertex), replicas[0].Crdt.Query().([]datatypes.Vertex)) {
				for i := 0; i < numReplicas; i++ {
					t.Error("Replica ", i, ": ", replicas[i].Crdt.Query())
				}
				return false
			}
		}

		return true
	}

	// Define generator to limit input size
	gen := func(vals []reflect.Value, rand *rand.Rand) {
		operations_rep0 := []communication.Operation{
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "h", Index: 0}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: " ", Index: 1}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "p", Index: 2}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "c", Index: 3}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "d", Index: 4}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "o", Index: 4}},
		}
		operations_rep1 := []communication.Operation{
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "e", Index: 0}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "e", Index: 1}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "d", Index: 2}},
			{Type: "Rem", Value: datatypes.RGAOpIndex{Value: nil, Index: 3}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "d", Index: 4}},
		}
		operations_rep2 := []communication.Operation{
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "l", Index: 1}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "l", Index: 1}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "o", Index: 2}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "a", Index: 3}},
			{Type: "Add", Value: datatypes.RGAOpIndex{Value: "s", Index: 4}},
		}

		operations := [][]communication.Operation{operations_rep0, operations_rep1, operations_rep2}
		vals[0] = reflect.ValueOf(operations)      //operations
		vals[1] = reflect.ValueOf(len(operations)) //number of replicas
		vals[2] = reflect.ValueOf(16)               //number of operations
	}

	// Define config for quick.Check
	config := &quick.Config{
		MaxCount: 1,
		Values:   gen,
	}

	// Generate and test random inputs
	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}
