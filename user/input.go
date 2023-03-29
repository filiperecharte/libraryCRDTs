package user

import (
	"bufio"
	"fmt"
	"library/packages/replica"
	"os"
	"strconv"
	"strings"
)

func RunInput(replicas []replica.Replica) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("INPUT: replica operation \n")
		// reads user input until \n by default
		scanner.Scan()
		// Holds the string that was scanned
		text := scanner.Text()
		if len(text) != 0 {
			input := strings.Split(text, " ")
			op, err := strconv.Atoi(input[1])
			rep, err := strconv.Atoi(input[0])

			if err != nil {
				fmt.Println("Invalid input")
				return
			}

			if rep > 0 && rep <= len(replicas) {
				if input[1] == "QUERY" {
					fmt.Println(replicas[rep-1].Query())
				} else {
					replicas[rep-1].Update(op)
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
