package test

import (
	"library/packages/datatypes/ecro/custom"
	"library/packages/replica"
	"math/rand"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"
)

func TestAuction(t *testing.T) {

	// Define property to test
	property := func(addusers []int, remusers []int, placebids []custom.Bid, close int, numReplicas int) bool {

		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		for i := 0; i < numReplicas; i++ {
			replicas[i] = custom.NewAuctionReplica(strconv.Itoa(i), channels, (numReplicas-1)*(len(addusers)+len(remusers)+len(placebids)+1))
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(4)
			go func(r *replica.Replica, adds []int) {
				defer wg.Done()
				// Perform random add operations
				for j := 0; j < len(adds); j++ {
					r.Prepare("AddUser", adds[rand.Intn(len(adds))])
				}
			}(replicas[i], addusers)

			go func(r *replica.Replica, rems []int) {
				defer wg.Done()
				// Perform random rem operations
				for j := 0; j < len(rems); j++ {
					r.Prepare("RemUser", rems[rand.Intn(len(rems))])
				}
			}(replicas[i], remusers)

			go func(r *replica.Replica) {
				defer wg.Done()
				// Perform random rem operations
				//sleep
				time.Sleep(time.Duration(close) * time.Millisecond)
				r.Prepare("Close", nil)
			}(replicas[i])

			go func(r *replica.Replica, rems []custom.Bid) {
				defer wg.Done()
				// Perform random rem operations
				for j := 0; j < len(rems); j++ {
					r.Prepare("PlaceBid", rems[rand.Intn(len(rems))])
				}
			}(replicas[i], placebids)

		}

		// Wait for all goroutines to finish
		wg.Wait()

		// Wait for all replicas to receive all messages
		for {
			flag := false
			for i := 0; i < numReplicas; i++ {
				if replicas[i].Crdt.NumOps() == uint64(numReplicas*(len(addusers)+len(remusers)+len(placebids)+1)) {
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
			st := replicas[i].Crdt.Query().(custom.AuctionState)
			stt := replicas[0].Crdt.Query().(custom.AuctionState)
			if !custom.CompareAuctionStates(st, stt) {
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

		addusers := []int{1, 2, 3, 4, 5}
		remusers := []int{1, 4, 5}
		placebids := []custom.Bid{{User: 3, Ammount: 1}, {User: 4, Ammount: 2}, {User: 5, Ammount: 3}}
		close := 1

		vals[0] = reflect.ValueOf(addusers)
		vals[1] = reflect.ValueOf(remusers)
		vals[2] = reflect.ValueOf(placebids)
		vals[3] = reflect.ValueOf(close)
		vals[4] = reflect.ValueOf(3)
	}

	// Define config for quick.Check
	config := &quick.Config{
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		MaxCount: 80,
		Values:   gen,
	}

	// Generate and test random inputs
	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}
