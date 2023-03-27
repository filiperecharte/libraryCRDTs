package utils

import "library/packages/middleware"

func MessagesToValues(l []interface{}) []interface{} {
	var list []interface{}
	for _, obj := range l {
		list = append(list, obj.(middleware.Message).Value)
	}
	return list
}

// get string keys from a map
func MapToKeys(m map[string]chan interface{}) []string {
	var list []string
	for obj, _ := range m {
		list = append(list, obj)
	}
	return list
}
