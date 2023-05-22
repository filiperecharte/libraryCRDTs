package main

import (
	"fmt"
	"library/packages/communication"
	"library/packages/datatypes/ecro/custom"
	"log"
	"strconv"
	"strings"

	mapset "github.com/deckarep/golang-set/v2"
)

type OperationValue struct {
	Id1 int //id of the user who is adding/removing
	Id2 int //id of the user who is being added/removed
}

func Orde(op1 communication.Operation, op2 communication.Operation) bool {
	//order map of operations by type of operation,
	//remFriend < addFriend
	//remRequest < addRequest
	//addFriend < addRequest
	// rmFriend and rmRequest are commutative

	return op1.Type == "RemFriend" && op2.Type == "AddFriend" ||
		op1.Type == "RemRequest" && op2.Type == "AddRequest" ||
		op1.Type == "AddRequest" && op2.Type == "AddRequest"
}

func Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == op2.Type ||
		op1.Value.(OperationValue).Id1 != op2.Value.(OperationValue).Id1 && op1.Value.(OperationValue).Id2 == op2.Value.(OperationValue).Id2 ||
		op1.Value.(OperationValue).Id1 == op2.Value.(OperationValue).Id1 && op1.Value.(OperationValue).Id2 != op2.Value.(OperationValue).Id2 ||
		op1.Value.(OperationValue).Id1 != op2.Value.(OperationValue).Id1 && op1.Value.(OperationValue).Id2 != op2.Value.(OperationValue).Id2
}

/*-------------------------------------*/

func Order(operations []communication.Operation) []communication.Operation {
	//order map of operations by type of operation, removes come before adds
	sortedOperations := make([]communication.Operation, len(operations))
	copy(sortedOperations, operations)

	for i := 0; i < len(sortedOperations); i++ {
		for j := i + 1; j < len(sortedOperations); j++ {
			//order by originID
			if sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Ancestor || sortedOperations[i].OriginID > sortedOperations[j].OriginID {
				sortedOperations[i], sortedOperations[j] = sortedOperations[j], sortedOperations[i]
			}
		}
	}

	log.Println("----SORTED OPERATIONS----")
	for _, op := range sortedOperations {
		log.Println(op)
	}

	for i := 1; i < len(sortedOperations); i++ {
		for j := i - 1; j >= 0; j-- {
			log.Println("[COMPARING]", sortedOperations[i], sortedOperations[j])
			if j == 0 {
				if i == j+1 {
					break
				}

				op := sortedOperations[i]

				log.Println("[SWAP END]", sortedOperations[i], sortedOperations[j])

				// Remove the element from the original position
				sortedOperations = append(sortedOperations[:i], sortedOperations[i+1:]...)

				// Insert the element at the new position
				sortedOperations = append([]communication.Operation{sortedOperations[0]}, append([]communication.Operation{op}, sortedOperations[1:]...)...)

				break
			}

			if sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Ancestor ||
				(sortedOperations[i].Concurrent(sortedOperations[j]) && Orde(sortedOperations[j], sortedOperations[i]) && !Commutes(sortedOperations[j], sortedOperations[i])) {
				if i == j+1 {
					log.Println("[BREAK]", sortedOperations[i], sortedOperations[j])
					break
				}

				op1 := sortedOperations[i]

				// Swap operations[i] and operations[j] if they meet the condition.
				log.Println("[SWAP]", sortedOperations[i], sortedOperations[j])
				// Remove the element from the original position
				sortedOperations = append(sortedOperations[:i], sortedOperations[i+1:]...)

				// Insert the element at the new position
				sortedOperations = append(sortedOperations[:j+1], append([]communication.Operation{op1}, sortedOperations[j+1:]...)...)

				break
			}
		}
	}

	return sortedOperations
}

/*----------------*/

func main() {

	//create two sets
	set1 := mapset.NewSet[any]()
	set2 := mapset.NewSet[any]()

	set1.Add(custom.OperationValue{Id1: 1, Id2: 2})
	set2.Add(custom.OperationValue{Id1: 1, Id2: 2})

	log.Println(set1.Equal(set2))

}

func parseMessage(input string) (communication.Operation, error) {
	var message communication.Operation

	// Parse type
	startIndex := strings.Index(input, "{") + 1
	endIndex := strings.Index(input, " ")
	if startIndex == -1 || endIndex == -1 {
		return message, fmt.Errorf("could not find type in input string")
	}
	message.Type = input[startIndex:endIndex]

	// Parse value
	startIndex = endIndex + 2 // skip space and opening bracket
	endIndex = strings.Index(input[startIndex:], "}") + startIndex
	if startIndex == -1 || endIndex == -1 {
		return message, fmt.Errorf("could not find value in input string")
	}
	valueString := input[startIndex:endIndex]
	valueString = strings.Replace(valueString, " ", ",", -1)
	valueInts := strings.Split(valueString, ",")

	id1, _ := strconv.Atoi(valueInts[0])
	id2, _ := strconv.Atoi(valueInts[1])
	message.Value = OperationValue{Id1: id1, Id2: id2}

	// Parse version
	startIndex = strings.Index(input, "[") + 1
	endIndex = strings.Index(input[startIndex:], "]") + startIndex

	versionString := input[startIndex:endIndex]
	versionParts := strings.Split(versionString, " ")
	versionMap := make(map[string]uint64)
	for i := 0; i < len(versionParts); i += 1 {
		v := strings.Split(versionParts[i], ":")
		key := v[0]
		value, _ := strconv.ParseUint(v[1], 10, 64)

		versionMap[key] = value
	}
	message.Version = communication.NewVClockFromMap(versionMap)

	startIndex = endIndex + 2 // skip space a bracket
	endIndex = strings.Index(input[startIndex:], "}") + startIndex

	originID := input[startIndex:endIndex]

	message.OriginID = originID

	return message, nil
}
