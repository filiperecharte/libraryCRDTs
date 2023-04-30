package test

import (
	"library/packages/datatypes"
	"library/packages/replica"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"
)

func TestCounter(t *testing.T) {

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
			replicas[i] = datatypes.NewCounterReplica(strconv.Itoa(i), channels, 2*len(adds))
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(1)
			go func(r *replica.Replica, adds []int) {
				defer wg.Done()
				// Perform random number of add operations
				for j := 0; j < len(adds); j++ {
					r.Prepare("Add", adds[j])
				}
			}(replicas[i], adds)
		}

		// Wait for all goroutines to finish
		wg.Wait()

		time.Sleep(5 * time.Second)

		// Check that all replicas have the same state
		for i := 1; i < numReplicas; i++ {
			if !reflect.DeepEqual(replicas[i].Crdt.Query(), replicas[0].Crdt.Query()) {
				for i := 0; i < numReplicas; i++ {
					t.Error("Replica ", i, ": ", replicas[i].Crdt.Query())
				}
				return false
			}
		}
		for i := 0; i < numReplicas; i++ {
			t.Log("Replica ", i, ": ", replicas[i].Crdt.Query())
		}
		return true
	}

	// Define generator to limit input size
	gen := func(vals []reflect.Value, rand *rand.Rand) {
		numAdds := 5
		adds := make([]int, numAdds)

		for i := 0; i < numAdds; i++ {
			adds = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
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
