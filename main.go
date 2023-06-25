package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"library/packages/communication"
	"log"
	"os"
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
	files, err := ioutil.ReadDir("./test/evaluation/results")
	if err != nil {
		log.Fatal(err)
	}

	var total float64 = 0
	var lineCount float64 = 0
	lineSums := make(map[int]float64)

	for _, file := range files {
		if strings.HasSuffix(file.Name(), "time.out") {
			f, err := os.Open("./test/evaluation/results" + file.Name())
			if err != nil {
				log.Fatal(err)
			}
			scanner := bufio.NewScanner(f)
			var lineNum int
			for scanner.Scan() {
				lineNum++
				line := scanner.Text()
				number, err := strconv.ParseFloat(strings.TrimSpace(line), 64)
				if err != nil {
					log.Fatal(err)
				}
				lineSums[lineNum] += number
				lineCount++
			}
			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
			f.Close()
		}
	}

	for _, sum := range lineSums {
		total += sum
	}

	average := total / lineCount

	fmt.Printf("The average of all numbers in 'time.out' files is: %v\n", average)
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
