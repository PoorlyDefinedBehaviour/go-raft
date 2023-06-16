package kv_test

import (
	"testing"

	"github.com/poorlydefinedbehaviour/raft-go/src/testing/cluster"
	"github.com/stretchr/testify/assert"
)

func TestBasicKv(t *testing.T) {
	t.Parallel()

	cluster := cluster.Setup()

	leader := cluster.MustWaitForLeader()

	assert.NoError(t, leader.Kv.Set("key", []byte("value")))
}
