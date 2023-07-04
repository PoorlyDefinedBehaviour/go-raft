package slicesx

func Map[T any, U any](xs []T, predicate func(*T) U) []U {
	out := make([]U, 0, len(xs))

	for _, x := range xs {
		out = append(out, predicate(&x))
	}

	return out
}

func Find[T any](xs []T, predicate func(*T) bool) (*T, bool) {
	for i := 0; i < len(xs); i++ {
		if predicate(&xs[i]) {
			return &xs[i], true
		}
	}

	return nil, false
}

func FindLast[T any](xs []T, predicate func(*T) bool) (*T, bool) {
	for i := len(xs) - 1; i >= 0; i-- {
		if predicate(&xs[i]) {
			return &xs[i], true
		}
	}

	return nil, false
}
