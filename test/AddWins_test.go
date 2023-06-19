package test

import (
	datatypes "library/packages/datatypes/semidirect"
	"library/packages/replica"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"
)

func TestAddWins(t *testing.T) {

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
			replicas[i] = datatypes.NewAddWins2Replica(strconv.Itoa(i), channels, numOperations-operations[i])
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(1)
			go func(r *replica.Replica, operations int) {
				defer wg.Done()
				// Perform random add operations
				for j := 0; j < operations; j++ {

					//generate random number
					n := rand.Intn(10)

					//choose randomly if it is an add or remove operation
					OPType := "Add"
					if rand.Intn(2) == 0 {
						OPType = "Add"
					} else {
						OPType = "Rem"
					}

					r.Prepare(OPType, n)
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
			st, _ := replicas[i].Crdt.Query()
			stt, _ := replicas[0].Crdt.Query()
			if !reflect.DeepEqual(st, stt) {
				for i := 0; i < numReplicas; i++ {
					st, _ := replicas[i].Crdt.Query()
					t.Error("Replica ", i, ": ", st)
				}
				return false
			}
		}
		for i := 0; i < numReplicas; i++ {
			st, _ := replicas[i].Crdt.Query()
			t.Log("Replica ", i, ": ", st)
		}
		return true
	}

	// Define generator to limit input size
	gen := func(vals []reflect.Value, rand *rand.Rand) {

		operations_rep0 := 20
		operations_rep1 := 20
		operations_rep2 := 20

		operations := []int{operations_rep0, operations_rep1, operations_rep2}
		vals[0] = reflect.ValueOf(operations)      //number of operations for each replica
		vals[1] = reflect.ValueOf(len(operations)) //number of replicas
		vals[2] = reflect.ValueOf(60)              //number of operations
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
