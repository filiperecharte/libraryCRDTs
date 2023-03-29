package crdts

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
