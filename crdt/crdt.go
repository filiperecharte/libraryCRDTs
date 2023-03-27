package crdt

type Crdt interface {
	// Return default (zero) state. Used to initialize CRDT state.
	Default() interface{}

	// Apply `operations` to a given `state`.
	// All `operations` are unstable.
	Apply(state interface{}, operations []interface{}) interface{}

	// Order unstable operations.
	//Order(state interface{}, operations map[interface{}]struct{}) interface{}

}
