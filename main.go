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
	replica1 := replica.NewReplica("1", crdts.Counter{}, channels, true)
	replica2 := replica.NewReplica("2", crdts.Counter{}, channels, false)
	replica3 := replica.NewReplica("3", crdts.Counter{}, channels, false)

	replicas := []*replica.Replica{replica1, replica2, replica3}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("INPUT: replica operation \n")
		// reads user input until \n by default
		scanner.Scan()
		// Holds the string that was scanned
		text := scanner.Text()
		if len(text) != 0 {
			input := strings.Split(text, " ")

			if rep, err2 := strconv.Atoi(input[0]); err2 == nil && rep > 0 && rep <= len(replicas) {
				if input[1] == "QUERY" {
					fmt.Println(replicas[rep-1].Query())
				} else {
					if op, err := strconv.Atoi(input[1]); err == nil {
						replicas[rep-1].Update(op)
					}
				}
			} else {
				fmt.Println("Invalid input")
				return
			}

		} else {
			// exit if user entered an empty string
			break
		}

	}
}
