package kv

import (
	"encoding/json"
	"fmt"

	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

const SetCommand uint8 = 1

type setCommandEntry[K comparable, V any] struct {
	Key   K `json:"key"`
	Value V `json:"value"`
}

type KvStore[K comparable, V any] struct {
	items map[K]V
}

func NewKvStore[K comparable, V any]() *KvStore[K, V] {
	return &KvStore[K, V]{items: make(map[K]V)}
}

func (kv *KvStore[K, V]) Apply(entry *types.Entry) error {
	switch entry.Type {
	case SetCommand:
		var command setCommandEntry[K, V]
		if err := json.Unmarshal(entry.Value, &command); err != nil {
			return fmt.Errorf("unmarshaling set command: entry.Value=%s %w", string(entry.Value), err)
		}

		kv.items[command.Key] = command.Value
	default:
		return fmt.Errorf("unknown entry type: entry=%+v", *entry)
	}

	return nil
}

func (kv *KvStore[K, V]) Get(key K) (V, bool) {
	value, ok := kv.items[key]
	return value, ok
}
