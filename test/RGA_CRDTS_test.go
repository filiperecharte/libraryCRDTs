package test

import (
	"library/packages/datatypes"
	commutative "library/packages/datatypes/commutative"
	crdtECRO "library/packages/datatypes/crdtECRO"
	ecro "library/packages/datatypes/ecro"
	"library/packages/replica"
	"log"
	"math/rand"
	_ "net/http/pprof"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"
)

// variable with the alphabet to generate random strings
var lettersCRDTS = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func TestRGACRDTS(t *testing.T) {
	// Define property to test
	property := func(operations []int, numReplicas int, numOperations int) bool {
		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		replicas[0] = crdtECRO.NewRGAReplica(strconv.Itoa(0), channels, numOperations-operations[0])
		replicas[1] = ecro.NewRGAReplica(strconv.Itoa(1), channels, numOperations-operations[1])
		replicas[2] = commutative.NewRGAReplica(strconv.Itoa(2), channels, numOperations-operations[2])

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(1)

			go func(r *replica.Replica, operations int) {
				defer wg.Done()
				// Perform random number of add operations with random delays

				for j := 0; j < operations; j++ {
					//choose a predecessor or a vertex to remove randomly from query
					rgaState, _ := r.Crdt.Query()
					v := rgaState.([]datatypes.Vertex)[rand.Intn(len(rgaState.([]datatypes.Vertex)))]

					//choose random leter to add
					value := lettersCRDTS[rand.Intn(len(lettersCRDTS))]

					//choose randomly if it is an add or remove operation
					OPType := ""
					if rand.Intn(2) == 0 {
						OPType = "Add"
					} else {
						OPType = "Rem"
						if v.Value == "" { //do not generate removes to the head of the list
							j--
							continue
						}
					}

					OPValue := datatypes.RGAOpValue{
						Value: value,
						V:     v,
					}

					r.Prepare(OPType, OPValue)

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

		log.Println("EFFECT OPS: ", replicas[0].Crdt.NumOps())
		log.Println("STABILIZED OPS: ", replicas[0].Crdt.NumSOps())

		//Check that all replicas have the same state
		for i := 1; i < numReplicas; i++ {
			st, _ := replicas[i].Crdt.Query()
			stt, _ := replicas[0].Crdt.Query()
			if !datatypes.RGAEqual(st.([]datatypes.Vertex), stt.([]datatypes.Vertex)) {
				for i := 0; i < numReplicas; i++ {
					t.Error("Replica ", i)
					q, _ := replicas[i].Crdt.Query()
					for j := 0; j < len(q.([]datatypes.Vertex)); j++ {
						log.Println(q.([]datatypes.Vertex)[j])
					}
				}
				return false
			}
		}
		log.Println("All replicas have the same state")
		return true
	}

	// Define generator to limit input size
	gen := func(vals []reflect.Value, rand *rand.Rand) {

		operations := []int{}
		for i := 0; i < 3; i++ {
			operations = append(operations, 10)
		}

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
