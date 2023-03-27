package main

import (
	"bufio"
	"fmt"
	"library/packages/middleware"
	"library/packages/replica"
	"os"
	"strconv"
	"strings"
)

type Counter struct{}

func (c Counter) Default() interface{} {
	return 0
}

func (c Counter) Apply(s interface{}, ops []interface{}) interface{} {
	state := s.(int)
	for _, op := range ops {
		state += op.(int)
	}
	return state
}

func main() {

	// create Replicas and assign CRDT
	replica1 := replica.NewReplica("1", []string{"1", "2", "3"}, Counter{})
	replica2 := replica.NewReplica("2", []string{"1", "2", "3"}, Counter{})
	replica3 := replica.NewReplica("3", []string{"1", "2", "3"}, Counter{})

	// create array with middlewares of replicas
	middlewares := []middleware.Middleware{*replica1.Middleware, *replica2.Middleware, *replica3.Middleware}

	// assign middleares array to each replica middlware middlewares
	replica1.Middleware.Middlewares = middlewares
	replica2.Middleware.Middlewares = middlewares
	replica3.Middleware.Middlewares = middlewares

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("INPUT: replica id, operation \n")
		// reads user input until \n by default
		scanner.Scan()
		// Holds the string that was scanned
		text := scanner.Text()
		if len(text) != 0 {
			input := strings.Split(text, ",")
			op, _ := strconv.Atoi(input[1])

			switch rep := input[0]; rep {
			case "1":
				if input[1] == "QUERY" {
					fmt.Println(replica1.Query())
				} else {
					replica1.Update(op)
				}
			case "2":
				if input[1] == "QUERY" {
					fmt.Println(replica2.Query())
				} else {
					replica2.Update(op)
				}
			case "3":
				if input[1] == "QUERY" {
					fmt.Println(replica3.Query())
				} else {
					replica3.Update(op)
				}
			case "QUERY":
				fmt.Println(replica1.Query())
			default:
				fmt.Println("Invalid input")
			}

		} else {
			// exit if user entered an empty string
			break
		}

	}
}
