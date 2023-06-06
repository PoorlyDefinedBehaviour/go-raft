package kv

import "github.com/poorlydefinedbehaviour/raft-go/src/types"

type KvStore struct{}

func NewKvStore() *KvStore {
	return &KvStore{}
}

func (kv *KvStore) Apply(entry *types.Entry) error {
	return nil
}
