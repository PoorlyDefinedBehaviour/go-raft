package testingrand

import (
	"github.com/poorlydefinedbehaviour/raft-go/src/rand"
)

func Choose[T any](rand rand.Random, xs []T) (T, bool) {
	if len(xs) == 0 {
		var zeroValue T
		return zeroValue, false
	}

	index := rand.GenBetween(0, uint64(len(xs)))

	return xs[int(index)], true
}
