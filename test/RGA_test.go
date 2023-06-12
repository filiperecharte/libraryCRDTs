package test

import (
	"encoding/csv"
	"library/packages/communication"
	datatypes "library/packages/datatypes/semidirect"
	"library/packages/replica"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/jmcvetta/randutil"
)

// variable with the alphabet to generate random strings
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func TestRGA(t *testing.T) {

	// Define property to test
	property := func(operations []int, numReplicas int, numOperations int) bool {

		file, err := os.Create("results/RGA" + "-" + strconv.Itoa(numReplicas) + "-" + strconv.Itoa(numOperations) + ".csv")
		if err != nil {
			log.Fatal(err)
		}
		w := csv.NewWriter(file)

		w.Write([]string{"Time"})

		// Initialize channels
		channels := map[string]chan interface{}{}
		for i := 0; i < numReplicas; i++ {
			channels[strconv.Itoa(i)] = make(chan interface{})
		}

		// Initialize replicas
		replicas := make([]*replica.Replica, numReplicas)
		for i := 0; i < numReplicas; i++ {
			replicas[i] = datatypes.NewRGAReplica(strconv.Itoa(i), channels, numOperations-operations[i], &w)
		}

		// Start a goroutine for each replica
		var wg sync.WaitGroup
		for i := range replicas {
			wg.Add(1)

			go func(r *replica.Replica, operations int) {
				defer wg.Done()
				// Perform random number of add operations with random delays

				for j := 0; j < operations; j++ {
					//choose a predecessor or a vertex to remove randomly from query
					v := generateRandomVertex(*r)

					//choose random leter to add
					value := letters[rand.Intn(len(letters))]

					//choose randomly if it is an add or remove operation
					OPType := "Add"
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

		w.Flush()
		file.Close()

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
		operations_rep0 := 5
		operations_rep1 := 5
		operations_rep2 := 5

		operations := []int{operations_rep0, operations_rep1, operations_rep2}
		vals[0] = reflect.ValueOf(operations)      //number of operations for each replica
		vals[1] = reflect.ValueOf(len(operations)) //number of replicas
		vals[2] = reflect.ValueOf(15)              //number of operations
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

func generateRandomVertex(r replica.Replica) datatypes.Vertex {
	rgaState, rgaDeletedState := r.Crdt.Query()

	v := datatypes.Vertex{}
	if len(rgaDeletedState.([]communication.Operation)) != 0 {
		v = rgaDeletedState.([]communication.Operation)[rand.Intn(len(rgaDeletedState.([]communication.Operation)))].Value.(datatypes.RGAOpValue).V
	} else {
		v = rgaState.([]datatypes.Vertex)[rand.Intn(len(rgaState.([]datatypes.Vertex)))]
	}

	choices := make([]randutil.Choice, 0, 2)
	choices = append(choices, randutil.Choice{Weight: 2, Item: rgaState.([]datatypes.Vertex)[rand.Intn(len(rgaState.([]datatypes.Vertex)))]})
	choices = append(choices, randutil.Choice{
		Weight: 5,
		Item:   v,
	})

	result, err := randutil.WeightedChoice(choices)
	if err != nil {
		panic(err)
	}

	return result.Item.(datatypes.Vertex)
}
