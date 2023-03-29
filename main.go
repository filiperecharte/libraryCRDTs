package main

import (
	"library/packages/crdts"
	"library/packages/replica"
	"library/packages/user"
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

	replicas := []replica.Replica{*replica1, *replica2, *replica3}

	user.RunInput(replicas)
}
