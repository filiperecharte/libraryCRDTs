package test

import (
	"library/packages/datatypes/custom"
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
	property := func(addtournaments []int, addplayers []int, remtournaments []int, remplayers []int, enroll []custom.Enroll, numReplicas int) bool {

		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		for i := 0; i < numReplicas; i++ {
			replicas[i] = custom.NewEgameReplica(strconv.Itoa(i), channels, (numReplicas-1)*(len(addtournaments)+len(addplayers)+len(remtournaments)+len(remplayers)+len(enroll)))
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(5)
			go func(r *replica.Replica, adds []int) {
				defer wg.Done()
				// Perform random add operations
				for j := 0; j < len(adds); j++ {
					r.Prepare("AddTournament", adds[rand.Intn(len(adds))])
				}
			}(replicas[i], addtournaments)

			go func(r *replica.Replica, rems []int) {
				defer wg.Done()
				// Perform random rem operations
				for j := 0; j < len(rems); j++ {
					r.Prepare("RemPlayer", rems[rand.Intn(len(rems))])
				}
			}(replicas[i], remplayers)

			go func(r *replica.Replica, rems []int) {
				defer wg.Done()
				// Perform random rem operations
				for j := 0; j < len(rems); j++ {
					r.Prepare("RemTournament", rems[rand.Intn(len(rems))])
				}
			}(replicas[i], remtournaments)

			go func(r *replica.Replica, adds []int) {
				defer wg.Done()
				// Perform random rem operations
				for j := 0; j < len(adds); j++ {
					r.Prepare("AddPlayer", adds[rand.Intn(len(adds))])
				}
			}(replicas[i], addplayers)

			go func(r *replica.Replica, adds []custom.Enroll) {
				defer wg.Done()
				// Perform random rem operations
				for j := 0; j < len(adds); j++ {
					r.Prepare("Enroll", adds[rand.Intn(len(adds))])
				}
			}(replicas[i], enroll)

		}

		// Wait for all goroutines to finish
		wg.Wait()

		// Wait for all replicas to receive all messages
		for {
			flag := false
			for i := 0; i < numReplicas; i++ {
				if replicas[i].Crdt.NumOps() == uint64(numReplicas*(len(addtournaments)+len(addplayers)+len(remtournaments)+len(remplayers)+len(enroll))) {
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
			st := replicas[i].Crdt.Query().(custom.EgameState)
			stt := replicas[0].Crdt.Query().(custom.EgameState)
			if !custom.CompareEgameStates(st, stt) {
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

		addtournaments := []int{1, 2, 3, 4}
		remtournaments := []int{1, 3}
		addplayers := []int{1, 2, 3, 4}
		remplayers := []int{2, 4}
		enroll := []custom.Enroll{{Player: 1, Tournament: 1}, {Player: 2, Tournament: 2}, {Player: 3, Tournament: 3}, {Player: 4, Tournament: 4}}

		vals[0] = reflect.ValueOf(addtournaments)
		vals[1] = reflect.ValueOf(addplayers)
		vals[2] = reflect.ValueOf(remtournaments)
		vals[3] = reflect.ValueOf(remplayers)
		vals[4] = reflect.ValueOf(enroll)
		vals[5] = reflect.ValueOf(5)
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
