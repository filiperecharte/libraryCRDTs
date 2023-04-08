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
	counter1 := crdts.NewCounter("1", channels, true)
	counter2 := crdts.NewCounter("2", channels, true)
	counter3 := crdts.NewCounter("3", channels, true)

	counters := []*replica.Replica{counter1, counter2, counter3}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("INPUT: replica operation \n")
		// reads user input until \n by default
		scanner.Scan()
		// Holds the string that was scanned
		text := scanner.Text()
		if len(text) != 0 {
			input := strings.Split(text, " ")

			if rep, err2 := strconv.Atoi(input[0]); err2 == nil && rep > 0 && rep <= len(counters) {
				if input[1] == "QUERY" {
					fmt.Println(counters[rep-1].Query())
				} else {
					if op, err := strconv.Atoi(input[1]); err == nil {
						counters[rep-1].Add(op)
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
