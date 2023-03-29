package crdts

import "library/packages/communication"

type Counter struct{}

func (c Counter) Default() interface{} {
	return 0
}

func (c Counter) Apply(s interface{}, ops []interface{}) interface{} {
	state := s.(int)
	for _, op := range ops {
		state += op.(communication.Message).Value.(int)
	}
	return state
}
