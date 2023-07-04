package kv

import (
	"encoding/json"
	"fmt"

	messagebus "github.com/poorlydefinedbehaviour/raft-go/src/message_bus"
	"github.com/poorlydefinedbehaviour/raft-go/src/types"
)

const SetCommand uint8 = 11

type setCommandEntry struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type KvStore struct {
	Items map[string][]byte
	bus   *messagebus.MessageBus
}

func New(bus *messagebus.MessageBus) *KvStore {
	return &KvStore{Items: make(map[string][]byte), bus: bus}
}

func (kv *KvStore) Set(key string, value []byte) (chan error, error) {
	bytes, err := json.Marshal(map[string]any{
		"key":   key,
		"value": value,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling key value pair: %w", err)
	}

	request := &types.UserRequestInput{
		Type:   SetCommand,
		Value:  bytes,
		DoneCh: make(chan error, 1),
	}

	kv.bus.ClientRequest(request)

	return request.DoneCh, nil
}

func (kv *KvStore) Apply(entry *types.Entry) error {
	switch entry.Type {
	case SetCommand:
		var command setCommandEntry
		if err := json.Unmarshal(entry.Value, &command); err != nil {
			return fmt.Errorf("unmarshaling set command: entry.Value=%s %w", string(entry.Value), err)
		}

		kv.Items[command.Key] = command.Value
	default:
		return fmt.Errorf("unknown entry type: entry=%+v", *entry)
	}

	return nil
}

func (kv *KvStore) Get(key string) ([]byte, bool) {
	value, ok := kv.Items[key]
	return value, ok
}
