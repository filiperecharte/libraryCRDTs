package main

import (
	"library/packages/communication"
	"log"
)

func Order(operations []communication.Operation) []communication.Operation {
	//order map of operations by type of operation, removes come before adds
	sortedOperations := make([]communication.Operation, len(operations))
	copy(sortedOperations, operations)

	for i := 0; i < len(sortedOperations); i++ {
		for j := i + 1; j < len(sortedOperations); j++ {
			if sortedOperations[i].Concurrent(sortedOperations[j]) && !(sortedOperations[i].Type == "Rem" && sortedOperations[j].Type == "Add") && !(sortedOperations[i].Type == sortedOperations[j].Type) {
				// Swap operations[i] and operations[j] if they meet the condition.
				sortedOperations[i], sortedOperations[j] = sortedOperations[j], sortedOperations[i]
			} else if sortedOperations[i].Version.Compare(sortedOperations[j].Version) == communication.Ancestor {
				sortedOperations[i], sortedOperations[j] = sortedOperations[j], sortedOperations[i]
			}
		}
	}

	return sortedOperations
}

func main() {

	//create array of operations
	operations := []communication.Operation{
		{Type: "Add", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 1, "2": 0})},
		{Type: "Add", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 2, "2": 0})},
		{Type: "Add", Value: 2, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 3, "2": 0})},
		{Type: "Add", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 4, "2": 0})},
		{Type: "Rem", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 5, "2": 0})},
		{Type: "Rem", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 6, "2": 0})},
		{Type: "Rem", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 7, "2": 0})},
		{Type: "Add", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 1, "1": 0, "2": 0})},
		{Type: "Add", Value: 1, Version: communication.NewVClockFromMap(map[string]uint64{"0": 2, "1": 0, "2": 0})},
		{Type: "Rem", Value: 1, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 0, "2": 1})},
		{Type: "Add", Value: 1, Version: communication.NewVClockFromMap(map[string]uint64{"0": 3, "1": 0, "2": 0})},
		{Type: "Add", Value: 4, Version: communication.NewVClockFromMap(map[string]uint64{"0": 4, "1": 0, "2": 0})},
		{Type: "Add", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 0, "2": 2})},
		{Type: "Add", Value: 2, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 1, "2": 3})},
		{Type: "Rem", Value: 1, Version: communication.NewVClockFromMap(map[string]uint64{"0": 5, "1": 0, "2": 0})},
		{Type: "Add", Value: 4, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 1, "2": 4})},
		{Type: "Rem", Value: 2, Version: communication.NewVClockFromMap(map[string]uint64{"0": 6, "1": 0, "2": 0})},
		{Type: "Rem", Value: 2, Version: communication.NewVClockFromMap(map[string]uint64{"0": 7, "1": 0, "2": 0})},
		{Type: "Add", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 1, "2": 5})},
		{Type: "Rem", Value: 3, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 1, "2": 6})},
		{Type: "Rem", Value: 1, Version: communication.NewVClockFromMap(map[string]uint64{"0": 0, "1": 1, "2": 7})},
	}

	//sort operations
	sortedOperations := make([]communication.Operation, len(operations))
	copy(sortedOperations, operations)
	newop := Order(sortedOperations)

	log.Println(operations)
	log.Println(newop)

}
