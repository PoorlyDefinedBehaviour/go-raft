package kv_test

import (
	"testing"

	testingcluster "github.com/poorlydefinedbehaviour/raft-go/src/testing/cluster"
	"github.com/stretchr/testify/assert"
)

func TestBasicKv(t *testing.T) {
	t.Parallel()

	cluster := testingcluster.Setup()

	leader := cluster.MustWaitForLeader()

	doneCh, err := leader.Kv.Set("key", []byte("value"))
	assert.NoError(t, err)

loop:
	for {
		select {
		case err := <-doneCh:
			assert.NoError(t, err)
			break loop
		default:
			cluster.Tick()
		}
	}

	value, ok := leader.Kv.Get("key")
	assert.True(t, ok)
	assert.Equal(t, []byte("value"), value)
}
