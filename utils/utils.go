package utils

import (
	"encoding/csv"
	"library/packages/communication"
	"strconv"
	"time"

	"github.com/dominikbraun/graph"
)

// get string keys from a map
func MapToKeys(m map[string]chan interface{}) []string {
	var list []string
	for obj, _ := range m {
		list = append(list, obj)
	}
	return list
}

// initialize map from array of keys with value 0
func InitMin(ids []string) map[string]string {
	vc := make(map[string]string)
	for _, id := range ids {
		vc[id] = ""
	}
	return vc
}

// MapValueExists returns true if the given value exists in the values of the map.
func MapValueExists(m map[string]string, value string) bool {
	for _, v := range m {
		if v == value || v == "" {
			return true
		}
	}
	return false
}

// check if array contains operation
func Contains(operations []communication.Operation, op communication.Operation) bool {
	for _, o := range operations {
		if op.Equals(o) {
			return true
		}
	}
	return false
}

/*------------------------------------- GRAPH ----------------------------------------*/

func findAllTopologicalOrders(graph *graph.Graph[string, communication.Operation], path []string, discovered map[string]bool, inDegrees map[string]int, N int, orders *[][]string) {
	// do for every vertex
	predecessorMap, _ := (*graph).PredecessorMap()
	adjacencyMap, _ := (*graph).AdjacencyMap()
	for vertexHash, _ := range predecessorMap {

		// proceed only if in-degree of current node is 0 and
		// current node is not processed yet
		if inDegrees[vertexHash] == 0 && !discovered[vertexHash] {
			// for every adjacent vertex u of v,
			// reduce in-degree of u by 1
			for adjacentVertex, _ := range adjacencyMap[vertexHash] {
				inDegrees[adjacentVertex]--
			}

			// include current node in the path
			// and mark it as discovered
			path = append(path, vertexHash)
			discovered[vertexHash] = true

			// recur
			findAllTopologicalOrders(graph, path, discovered, inDegrees, N, orders)

			// backtrack: reset in-degree
			// information for the current node
			for adjacentVertex, _ := range adjacencyMap[vertexHash] {
				inDegrees[adjacentVertex]++
			}

			// backtrack: remove current node from the path and
			// mark it as undiscovered
			path = path[:len(path)-1]
			discovered[vertexHash] = false
		}
	}

	// add the topological order to orders if
	// all vertices are included in the path
	if len(path) == N {
		newPath := make([]string, len(path))
		copy(newPath, path)
		*orders = append(*orders, newPath)
	}
}

func GetAllTopologicalOrders(graph *graph.Graph[string, communication.Operation]) [][]string {
	// get number of nodes in the graph
	N, _ := (*graph).Order()

	// create an auxiliary space to keep track of whether vertex is discovered
	discovered := make(map[string]bool)

	inDegrees := make(map[string]int)
	predecessorMap, _ := (*graph).PredecessorMap()
	for v, edges := range predecessorMap {
		inDegrees[v] = len(edges)
		discovered[v] = false
	}

	// slice to store the topological order
	var path []string

	// slice to store all topological orders
	var orders [][]string

	// find all topological ordering and store them in orders
	findAllTopologicalOrders(graph, path, discovered, inDegrees, N, &orders)

	return orders
}

/*------------------------------------- TIME MEASURE ----------------------------------------*/

func Timer(w **csv.Writer) func() {
	start := time.Now()
	return func() {
		row := []string{strconv.FormatInt(time.Since(start).Microseconds(), 10)}
		(*w).Write(row)
	}
}
