package main

import (
	"library/packages/crdt"
)

type Counter struct{}

func (c Counter) Default() interface{} {
	return 0
}

func (c Counter) Apply(s interface{}, ops map[interface{}]struct{}) interface{} {
	state := s.(int64)
	for op, _ := range ops {
		state += op.(int64)
	}
	return state
}

func main() {

	// create CRDT
	var c crdt.Crdt = Counter{}

	// create replica
	// TODO

	// submit operations

}
