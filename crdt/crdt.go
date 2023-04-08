package crdt

import (
	"library/packages/communication"
)

type Crdt interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	//Apply(state interface{}, operations []interface{}) interface{}

	// Order unstable operations.
	//Order(state interface{}, operations map[interface{}]struct{}) interface{}

	// The TCDeliver callback function is called when a message is ready to be delivered.
	TCDeliver(msg communication.Message)

	// The TCStable callback function is called when a message is set to stable.
	TCStable(msg communication.Message)

	// Query made by a client to a replica that returns the current state of the CRDT
	// after applying the unstable operations into the CRDT stable state
	Query() interface{}
}
