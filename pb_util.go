package wal

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// MarshalEntry marshals the WAL_Entry into a byte slice.
func MarshalEntry(entity *WAL_Entry) []byte {
	// marshal the entity
	marshalEntry, err := proto.Marshal(entity)
	if err != nil {
		panic(fmt.Sprintf("error marshalling the entity: %v", err))
	}
	return marshalEntry
}

// UnmarshalEntry unmarshal the byte slice into a WAL_Entry.
func UnmarshalEntry(data []byte) *WAL_Entry {
	// unmarshal the data into the entity
	var entity WAL_Entry
	if err := proto.Unmarshal(data, &entity); err != nil {
		panic(fmt.Sprintf("error unmarshalling the data: %v", err))
	}
	return &entity
}
