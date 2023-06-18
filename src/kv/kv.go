package kv

import (
	"encoding/json"
	"fmt"

	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

const SetCommand uint8 = 2

type setCommandEntry struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type KvStore struct {
	items map[string][]byte
	bus   *messagebus.MessageBus
}

func NewKvStore(bus *messagebus.MessageBus) *KvStore {
	return &KvStore{items: make(map[string][]byte), bus: bus}
}

func (kv *KvStore) Set(key string, value []byte) error {
	bytes, err := json.Marshal(map[string]any{
		"key":   key,
		"value": value,
	})
	if err != nil {
		return fmt.Errorf("marshaling key value pair: %w", err)
	}

	request := types.UserRequestInput{
		Type:   SetCommand,
		Value:  bytes,
		DoneCh: make(chan error),
	}
	kv.bus.QueueUserRequest("TODO", &request)
	defer close(request.DoneCh)

	if err := <-request.DoneCh; err != nil {
		return fmt.Errorf("setting key value pair: %w", err)
	}

	return nil
}

func (kv *KvStore) Apply(entry *types.Entry) error {
	switch entry.Type {
	case SetCommand:
		var command setCommandEntry
		if err := json.Unmarshal(entry.Value, &command); err != nil {
			return fmt.Errorf("unmarshaling set command: entry.Value=%s %w", string(entry.Value), err)
		}

		kv.items[command.Key] = command.Value
	default:
		return fmt.Errorf("unknown entry type: entry=%+v", *entry)
	}

	return nil
}

func (kv *KvStore) Get(key string) ([]byte, bool) {
	value, ok := kv.items[key]
	return value, ok
}
