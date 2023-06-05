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

func TestEGames(t *testing.T) {

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
			replicas[i] = custom.NewEgameReplica(strconv.Itoa(i), channels, numOperations-operations[i])
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

					//choose randomly between addPlayer remPlayer addTournament remTournament enroll
					OPType := ""
					switch rand.Intn(5) {
					case 0:
						OPType = "AddPlayer"
						r.Prepare(OPType, OPValue)
					case 1:
						OPType = "RemPlayer"
						r.Prepare(OPType, OPValue)
					case 2:
						OPType = "AddTournament"
						r.Prepare(OPType, OPValue)
					case 3:
						OPType = "RemTournament"
						r.Prepare(OPType, OPValue)
					case 4:
						OPType = "Enroll"
						q, _ := r.Crdt.Query()
						players := q.(custom.EgameState).Players.ToSlice()
						tournaments := q.(custom.EgameState).Tournaments.ToSlice()
						if len(players) == 0 || len(tournaments) == 0 { //do not generate place bids when there are no users
							j--
							continue
						}
						player := players[rand.Intn(len(players))].(int)
						tournament := tournaments[rand.Intn(len(tournaments))].(int)

						OPValue := custom.Enroll{Player: player, Tournament: tournament}
						r.Prepare(OPType, OPValue)
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
			if !custom.CompareEgameStates(st.(custom.EgameState), stt.(custom.EgameState)) {
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
		MaxCount: 80,
		Values:   gen,
	}

	// Generate and test random inputs
	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}
