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

func TestSocial(t *testing.T) {

	// Define property to test
	property := func(addrequests []custom.OperationValue, addfriends []custom.OperationValue, remfriends []custom.OperationValue, remrequests []custom.OperationValue, numReplicas int) bool {

		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		for i := 0; i < numReplicas; i++ {
			replicas[i] = custom.NewSocialReplica(strconv.Itoa(i), channels, (numReplicas-1)*(len(addrequests)+len(addfriends)+len(remfriends)+len(remrequests)))
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(4)
			go func(r *replica.Replica, adds []custom.OperationValue) {
				defer wg.Done()
				// Perform random add operations
				for j := 0; j < len(adds); j++ {
					r.Prepare("AddFriend", adds[rand.Intn(len(adds))])
				}
			}(replicas[i], addfriends)

			go func(r *replica.Replica, rems []custom.OperationValue) {
				defer wg.Done()
				// Perform random rem operations
				for j := 0; j < len(rems); j++ {
					r.Prepare("RemFriend", rems[rand.Intn(len(rems))])
				}
			}(replicas[i], remfriends)

			go func(r *replica.Replica, rems []custom.OperationValue) {
				defer wg.Done()
				// Perform random rem operations
				for j := 0; j < len(rems); j++ {
					r.Prepare("AddRequest", rems[rand.Intn(len(rems))])
				}
			}(replicas[i], addrequests)

			go func(r *replica.Replica, rems []custom.OperationValue) {
				defer wg.Done()
				// Perform random rem operations
				for j := 0; j < len(rems); j++ {
					r.Prepare("RemRequest", rems[rand.Intn(len(rems))])
				}
			}(replicas[i], remrequests)

		}

		// Wait for all goroutines to finish
		wg.Wait()

		// Wait for all replicas to receive all messages
		for {
			flag := false
			for i := 0; i < numReplicas; i++ {
				if replicas[i].Crdt.NumOps() == uint64(numReplicas*(len(addrequests)+len(addfriends)+len(remfriends)+len(remrequests))) {
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
			st := replicas[i].Crdt.Query().(custom.SocialState)
			stt := replicas[0].Crdt.Query().(custom.SocialState)
			if !custom.CompareSocialStates(st, stt) {
				for i := 0; i < numReplicas; i++ {
					t.Error("Replica ", i, ": ", replicas[i].Crdt.Query())
				}
				return false
			}
		}
		for i := 0; i < numReplicas; i++ {
			t.Log("Replica ", i, ": ", replicas[i].Crdt.Query())
			// replicas[i].Quit()
			// close(channels[strconv.Itoa(i)])
		}

		return true
	}

	// Define generator to limit input size
	gen := func(vals []reflect.Value, rand *rand.Rand) {

		addrequests := []custom.OperationValue{{Id1: 1, Id2: 2}, {Id1: 3, Id2: 1}, {Id1: 0, Id2: 1}}
		addfriends := []custom.OperationValue{{Id1: 2, Id2: 1}, {Id1: 3, Id2: 1}, {Id1: 3, Id2: 2}, {Id1: 1, Id2: 3}}
		remfriends := []custom.OperationValue{{Id1: 3, Id2: 1}, {Id1: 0, Id2: 1}}
		remrequests := []custom.OperationValue{{Id1: 4, Id2: 1}}

		vals[0] = reflect.ValueOf(addrequests)
		vals[1] = reflect.ValueOf(addfriends)
		vals[2] = reflect.ValueOf(remfriends)
		vals[3] = reflect.ValueOf(remrequests)
		vals[4] = reflect.ValueOf(3)
	}

	// Define config for quick.Check
	config := &quick.Config{
		Rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		MaxCount: 200,
		Values:   gen,
	}

	// Generate and test random inputs
	if err := quick.Check(property, config); err != nil {
		t.Error(err)
	}
}
