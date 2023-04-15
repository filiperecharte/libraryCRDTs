package crdt

type semidirectDataI interface {

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state interface{}, operations []interface{}) interface{}

	// Repairs unstable operations.
	Repair(state interface{}, operations map[interface{}]struct{}) interface{}
}
