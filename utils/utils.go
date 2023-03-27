package utils

import "library/packages/middleware"

func MessagesToValues(l []interface{}) []interface{} {
	var list []interface{}
	for _, obj := range l {
		list = append(list, obj.(middleware.Message).Value)
	}
	return list
}
