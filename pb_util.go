package wal

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// MarshalEntry marshals the WAL_Entry into a byte slice.
func MarshalEntry(entry *WAL_Entry) []byte {
	// marshal the entity
	marshalEntry, err := proto.Marshal(entry)
	if err != nil {
		panic(fmt.Sprintf("error marshalling the entity: %v", err))
	}
	return marshalEntry
}

// UnmarshalEntry unmarshal the byte slice into a WAL_Entry.
func UnmarshalEntry(data []byte, entry *WAL_Entry) {
	if err := proto.Unmarshal(data, entry); err != nil {
		panic(fmt.Sprintf("error unmarshalling the data: %v", err))
	}
}
