package test

import (
	datatypes "library/packages/datatypes/ecro"
	"library/packages/replica"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
)

func TestMVRegister(t *testing.T) {

	// Define property to test
	property := func(adds []int, numReplicas int) bool {

		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		for i := 0; i < numReplicas; i++ {
			replicas[i] = datatypes.NewMVRegisterReplica(strconv.Itoa(i), channels, (numReplicas-1)*len(adds))
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(1)
			go func(r *replica.Replica, adds []int) {
				defer wg.Done()
				// Perform random number of add operations with random delays
				for j := 0; j < len(adds); j++ {
					k, _ := strconv.Atoi(r.GetID())
					r.Prepare("Add", k*5+j)
				}
			}(replicas[i], adds)
		}

		// Wait for all goroutines to finish
		wg.Wait()

		// Wait for all replicas to receive all messages
		for {
			flag := false
			for i := 0; i < numReplicas; i++ {
				if replicas[i].Crdt.NumOps() == uint64(numReplicas*(len(adds))) {
					flag = true
				} else {
					flag = false
					break
				}
			}
			if flag {
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
		numAdds := 5
		adds := make([]int, numAdds)

		for i := 0; i < numAdds; i++ {
			adds[i] = rand.Intn(10)
		}

		vals[0] = reflect.ValueOf(adds)
		vals[1] = reflect.ValueOf(3)
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
