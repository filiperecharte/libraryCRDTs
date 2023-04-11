package utils

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
