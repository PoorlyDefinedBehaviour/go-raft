package mapx

import "golang.org/x/exp/constraints"

func anyKey[K comparable, V constraints.Ordered](m map[K]V) (*K, bool) {
	for key := range m {
		return &key, true
	}
	return nil, false
}

func MinValue[K comparable, V constraints.Ordered](m map[K]V) (*V, bool) {
	key, ok := anyKey(m)
	if !ok {
		return nil, false
	}

	minValue := m[*key]
	min := &minValue

	for _, value := range m {
		if value < *min {
			min = &value
		}
	}

	return min, true
}
