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
	property := func(operations []int, numReplicas int, numOperations int) bool {

		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		for i := 0; i < numReplicas; i++ {
			replicas[i] = custom.NewAuctionReplica(strconv.Itoa(i), channels, numOperations-operations[i])
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
					OPValue := rand.Intn(10)

					//choose randomly between addUser remUser placeBid and close
					OPType := ""
					switch rand.Intn(4) {
					case 0:
						OPType = "AddUser"
						r.Prepare(OPType, OPValue)
					case 1:
						OPType = "RemUser"
						r.Prepare(OPType, OPValue)
					case 2:
						OPType = "PlaceBid"

						q, _ := r.Crdt.Query()
						users := q.(custom.AuctionState).Users.ToSlice()
						if len(users) == 0 { //do not generate place bids when there are no users
							j--
							continue
						}
						user := users[rand.Intn(len(users))].(int)

						OPValue := custom.Bid{User: user, Ammount: rand.Intn(100)}
						r.Prepare(OPType, OPValue)
					case 3:
						OPType = "Close"
						r.Prepare(OPType, nil)
					}
				}
			}(replicas[i], operations[i])

		}

		// Wait for all goroutines to finish
		wg.Wait()

		// Wait for all replicas to receive all messages
		for {
			flag := false
			for i := 0; i < numReplicas; i++ {
				if replicas[i].Crdt.NumOps() == uint64(numOperations) {
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
			if !custom.CompareAuctionStates(st.(custom.AuctionState), stt.(custom.AuctionState)) {
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

		operations_rep0 := 10
		operations_rep1 := 10
		operations_rep2 := 10

		operations := []int{operations_rep0, operations_rep1, operations_rep2}
		vals[0] = reflect.ValueOf(operations)      //number of operations for each replica
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
