package crdts

type MVRegister struct{}

func (a MVRegister) Default() interface{} {
	return make([]interface{}, 0)
}

func (a MVRegister) Apply(s interface{}, ops []interface{}) interface{} {
	state := s.(int)
	// check if there are concurrent operations using vector clocks and join them in a set

	return state
}
