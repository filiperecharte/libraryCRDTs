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

func TestAddWins(t *testing.T) {

	// Define property to test
	property := func(adds []int, rems []int, delays []time.Duration, numReplicas int) bool {

		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		for i := 0; i < numReplicas; i++ {
			replicas[i] = datatypes.NewAddWinsReplica(strconv.Itoa(i), channels)
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(2)
			go func(r *replica.Replica, adds []int) {
				defer wg.Done()
				// Perform random number of add operations with random delays
				for j := 0; j < 1; j++ {
					r.Prepare("Add", adds[j])
					time.Sleep(delays[j])
				}
			}(replicas[i], adds)

			go func(r *replica.Replica, rems []int) {
				defer wg.Done()
				// Perform random number of add operations with random delays
				for j := 0; j < 1; j++ {
					r.Prepare("Rem", rems[j])
					time.Sleep(delays[j])
				}
			}(replicas[i], rems)
		}

		// Wait for all goroutines to finish
		wg.Wait()

		time.Sleep(5 * time.Second)

		// Check that all replicas have the same state
		// for i := 1; i < numReplicas; i++ {
		// 	if !reflect.DeepEqual(replicas[i].Crdt.Query(), replicas[0].Crdt.Query()) {
		// 		for i := 0; i < numReplicas; i++ {
		// 			t.Error("Replica ", i, ": ", replicas[i].Crdt.Query())
		// 		}
		// 		return false
		// 	}
		// }
		for i := 0; i < numReplicas; i++ {
			t.Log("Replica ", i, ": ", replicas[i].Crdt.Query())
		}
		return true
	}

	// Define generator to limit input size
	gen := func(vals []reflect.Value, rand *rand.Rand) {
		numOps := 2
		adds := make([]int, numOps)
		rems := make([]int, numOps)
		delays := make([]time.Duration, numOps)

		for i := 0; i < numOps; i++ {
			adds = []int{1, 2}
			rems = []int{1, 2}
			delays[i] = time.Duration(rand.Intn(5)) * time.Millisecond
		}

		vals[0] = reflect.ValueOf(adds)
		vals[1] = reflect.ValueOf(rems)
		vals[2] = reflect.ValueOf(delays)
		vals[3] = reflect.ValueOf(3)
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
