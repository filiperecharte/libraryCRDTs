package main

import (
	"bufio"
	"fmt"
	"library/packages/crdts"
	"library/packages/replica"
	"os"
	"strconv"
	"strings"
)

func main() {

	var channels = map[string]chan interface{}{
		"1": make(chan interface{}),
		"2": make(chan interface{}),
		"3": make(chan interface{}),
	}

	// create Replicas and assign CRDT
	counter1 := crdts.NewMVRegister("1", channels, true)
	counter2 := crdts.NewMVRegister("2", channels, true)
	counter3 := crdts.NewMVRegister("3", channels, true)

	counters := []*replica.Replica{counter1, counter2, counter3}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("Enter command:")
		// Read the next line of input from the scanner
		if !scanner.Scan() {
			// An error occurred or end of input was reached
			fmt.Println("Error reading input:", scanner.Err())
			continue
		}

		// Parse the input line into arguments
		args := strings.Fields(scanner.Text())

		// Ensure at least two arguments are provided
		if len(args) < 2 {
			fmt.Println("Usage: <replica> <action> [<value>]")
			continue
		}

		// Parse replica number from first argument
		replica, err := strconv.Atoi(args[0])
		if err != nil {
			fmt.Println("Error: Invalid replica number")
			continue
		}

		// Parse action from second argument
		action := args[1]

		// Perform action based on second argument
		switch action {
		case "QUERY":
			fmt.Printf("Querying replica %d...\n", replica)
			fmt.Println(counters[replica-1].Query())
		case "ADD":
			if len(args) != 3 {
				fmt.Println("Error: Missing value argument for ADD action")
				continue
			}
			value, err := strconv.Atoi(args[2])
			if err != nil {
				fmt.Println("Error: Invalid value for ADD action")
				continue
			}
			fmt.Printf("Adding value %d to replica %d...\n", value, replica)
			counters[replica-1].Add(value)
		case "REM":
			if len(args) != 3 {
				fmt.Println("Error: Missing value argument for REM action")
				continue
			}
			value, err := strconv.Atoi(args[2])
			if err != nil {
				fmt.Println("Error: Invalid value for REM action")
				continue
			}
			fmt.Printf("Removing value %d from replica %d...\n", value, replica)
			counters[replica-1].Remove(value)
		default:
			fmt.Println("Error: Invalid action")
			break
		}
	}
}
