package main

import (
	"fmt"
	"library/packages/communication"
	"log"
	"strconv"
	"strings"
)

type OperationValue struct {
	Id1 int //id of the user who is adding/removing
	Id2 int //id of the user who is being added/removed
}

func Orde(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == "Rem" && op2.Type == "Add"
}

func Commutes(op1 communication.Operation, op2 communication.Operation) bool {
	return op1.Type == op2.Type || op1.Value != op2.Value
}

/*-------------------------------------*/

func Order(operations []communication.Operation) []communication.Operation {
	//order map of operations by type of operation, removes come before adds
	sortedOperations := make([]communication.Operation, len(operations))
	copy(sortedOperations, operations)

	for i := 0; i < len(sortedOperations); i++ {
		for j := i + 1; j < len(sortedOperations); j++ {
			//order by originID
			if sortedOperations[i].OriginID > sortedOperations[j].OriginID {
				sortedOperations[i], sortedOperations[j] = sortedOperations[j], sortedOperations[i]
			} else if sortedOperations[i].OriginID == sortedOperations[j].OriginID && sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Ancestor {
				sortedOperations[i], sortedOperations[j] = sortedOperations[j], sortedOperations[i]
			}
		}
	}

	log.Println("----SORTED OPERATIONS----")
	for _, op := range sortedOperations {
		log.Println(op)
	}

	for i := len(sortedOperations) - 2; i >= 0; i-- {
		for j := i + 1; j < len(sortedOperations); j++ {
			if sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Descendant || (sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Concurrent && Orde(sortedOperations[i], sortedOperations[j]) && !Commutes(sortedOperations[i], sortedOperations[j])) {
				if i+1 == j {
					break
				}
				op1 := sortedOperations[i]
				sortedOperations = append(sortedOperations[:i], sortedOperations[i+1:]...)
				sortedOperations = append(sortedOperations[:j-1], append([]communication.Operation{op1}, sortedOperations[j-1:]...)...)
				break
			}
			if j == len(sortedOperations)-1 {
				if i+1 == j {
					break
				}
				op1 := sortedOperations[i]
				sortedOperations = append(sortedOperations[:i], sortedOperations[i+1:]...)
				sortedOperations = append(sortedOperations, op1)
				break
			}
		}
	}

	return sortedOperations
}

/*----------------*/

func main() {

	//create two sets
	ops := []string{
		"{Rem 5 {0x14001738300 map[0:0 1:0 2:2]} 2}",
		"{Rem 1 {0x14001739158 map[0:0 1:0 2:3]} 2}",
		"{Add 2 {0x14001d79f20 map[0:0 1:1 2:0]} 1}",
		"{Add 2 {0x14001d79fe0 map[0:0 1:2 2:0]} 1}",
		"{Add 2 {0x14001739fe0 map[0:0 1:3 2:0]} 1}",
		"{Add 3 {0x140021ac990 map[0:0 1:4 2:0]} 1}",
		"{Add 5 {0x14001e44408 map[0:0 1:5 2:0]} 1}",
		"{Rem 5 {0x140021acff0 map[0:1 1:6 2:0]} 1}",
		"{Rem 5 {0x14001e447e0 map[0:1 1:7 2:0]} 1}",
		"{Rem 5 {0x14001e447f8 map[0:1 1:8 2:0]} 1}",
		"{Add 5 {0x14001d79cf8 map[0:2 1:0 2:0]} 0}",
		"{Add 4 {0x14001d79e90 map[0:3 1:0 2:0]} 0}",
		"{Add 3 {0x14001d79ea8 map[0:4 1:0 2:0]} 0}",
		"{Add 2 {0x14001739788 map[0:5 1:0 2:0]} 0}",
		"{Rem 1 {0x14001e44318 map[0:6 1:1 2:0]} 0}",
		"{Rem 4 {0x14001e44330 map[0:7 1:1 2:0]} 0}",
		"{Add 4 {0x140021ac9c0 map[0:1 1:0 2:4]} 2}",
		"{Rem 1 {0x14001e44498 map[0:8 1:1 2:0]} 0}",
		"{Add 1 {0x14001e44288 map[0:1 1:0 2:5]} 2}",
		"{Add 3 {0x14001e44810 map[0:1 1:0 2:6]} 2}",
		"{Add 3 {0x14001e447b0 map[0:1 1:0 2:7]} 2}",
		"{Add 3 {0x14001e451b8 map[0:1 1:0 2:8]} 2}",
		// "{Add 4 {0x140001281c8 map[0:1 1:0 2:4]}  2}",
		// "{Rem 1 {0x140001281e0 map[0:8 1:1 2:0]}  0}",
		// "{Add 1 {0x140001281f8 map[0:1 1:0 2:5]}  2}",
		// "{Add 3 {0x14000128210 map[0:1 1:0 2:6]}  2}",
		// "{Add 3 {0x14000128228 map[0:1 1:0 2:7]}  2}",
		// "{Add 3 {0x14000128240 map[0:1 1:0 2:8]}  2}",
		// "{Add 2 {0x14000128078 map[0:0 1:1 2:0]}  1}",
		// "{Add 2 {0x14000128090 map[0:0 1:2 2:0]}  1}",
		// "{Add 2 {0x140001280a8 map[0:0 1:3 2:0]}  1}",
		// "{Add 3 {0x140001280c0 map[0:0 1:4 2:0]}  1}",
		// "{Add 5 {0x140001280d8 map[0:0 1:5 2:0]}  1}",
		// "{Rem 5 {0x140001280f0 map[0:1 1:6 2:0]}  1}",
		// "{Rem 5 {0x14000128108 map[0:1 1:7 2:0]}  1}",
		// "{Rem 5 {0x14000128120 map[0:1 1:8 2:0]}  1}",
	}

	operations := []communication.Operation{}
	for _, op := range ops {
		o, _ := parseMessage(op)
		operations = append(operations, o)
	}

	//sort operations
	operations = Order(operations)

	fmt.Println("----SORTED OPERATIONS----")
	for _, op := range operations {
		fmt.Println(op)
	}

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
	startIndex = endIndex + 1
	endIndex = strings.Index(input[startIndex:], " ") + startIndex
	if startIndex == -1 || endIndex == -1 {
		return message, fmt.Errorf("could not find value in input string")
	}

	valueString := input[startIndex:endIndex]
	message.Value, _ = strconv.ParseInt(valueString, 10, 64)

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
