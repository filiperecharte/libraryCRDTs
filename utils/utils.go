package utils

import "library/packages/communication"

// get string keys from a map
func MapToKeys(m map[string]chan interface{}) []string {
	var list []string
	for obj, _ := range m {
		list = append(list, obj)
	}
	return list
}

// initialize map from array of keys with value 0
func InitMin(ids []string) map[string]string {
	vc := make(map[string]string)
	for _, id := range ids {
		vc[id] = ""
	}
	return vc
}

// MapValueExists returns true if the given value exists in the values of the map.
func MapValueExists(m map[string]string, value string) bool {
	for _, v := range m {
		if v == value || v == "" {
			return true
		}
	}
	return false
}

// check if array contains operation
func Contains(operations []communication.Operation, op communication.Operation) bool {
	for _, o := range operations {
		if op.Equals(o) {
			return true
		}
	}
	return false
}
