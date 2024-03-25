package tests

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/alecthomas/assert/v2"
	wal "github.com/danish45007/GoLogMatrix"
)

const (
	maxSegments = 3
	maxFileSize = 64 * 1000 * 1000 // 64MB
)

func TestWAL_WriteAndRecover(t *testing.T) {
	// run the test in parallel
	t.Parallel()
	// Setup: Create a temporary file for the WAL
	dirPath := "TestWAL_WriteAndRecover.log"
	defer os.RemoveAll(dirPath) // Cleanup after the test

	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to create WAL")
	defer walog.Close()

	// test data to write mimicking the data that would be written by the key-value store
	entries := []Record{
		{Key: "key1", Value: []byte("value1"), Op: InsertionOperation},
		{Key: "key2", Value: []byte("value2"), Op: InsertionOperation},
		{Key: "key3", Op: DeletionOperation},
	}
	// write the test data to WAL
	for _, entry := range entries {
		// marshal the record before writing
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntity(marshaledEntry), "Failed to write entry")
	}

	// recover the data from the WAL
	recoveredEntry, err := walog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")
	// check if the recovered entries matches with written entries
	for entryIndex, entry := range recoveredEntry {
		unMarshalledEntry := Record{}
		assert.NoError(t, json.Unmarshal(entry.Data, &unMarshalledEntry), "Failed to unmarshal entry")
		// can't use deep equality due the sequence number
		assert.Equal(t, entries[entryIndex].Key, unMarshalledEntry.Key, "Recovered entry does not match written entry (Key)")
		assert.Equal(t, entries[entryIndex].Op, unMarshalledEntry.Op, "Recovered entry does not match written entry (Op)")
		assert.True(t, reflect.DeepEqual(entries[entryIndex].Value, unMarshalledEntry.Value), "Recovered entry does not match written entry (Value)")
	}
}

// Test to verify is the log sequence number is increasing
// after reopening the WAL
func TestWAL_LogSequenceNumber(t *testing.T) {
	t.Parallel()
	dirPath := "TestWAL_LogSequenceNumber.log"
	defer os.RemoveAll(dirPath)

	wallog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to create WAL")

	// test data to write mimicking the data that would be written by the key-value store
	entries := []Record{
		{Key: "key1", Value: []byte("value1"), Op: InsertionOperation},
		{Key: "key2", Value: []byte("value2"), Op: InsertionOperation},
		{Key: "key3", Op: DeletionOperation},
		{Key: "key4", Value: []byte("value4"), Op: InsertionOperation},
		{Key: "key5", Value: []byte("value5"), Op: InsertionOperation},
		{Key: "key6", Op: DeletionOperation},
	}

	// write first 3 entries to WAL
	for i := 0; i < 3; i++ {
		marshaledEntry, err := json.Marshal(entries[i])
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, wallog.WriteEntity(marshaledEntry), "Failed to write entry")
	}

	// close the WAL
	assert.NoError(t, wallog.Close(), "Failed to close WAL")

	// reopen the WAL
	wallog, err = wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to reopen WAL")
	// write next 3 entries to WAL
	for i := 3; i < 6; i++ {
		marshaledEntry, err := json.Marshal(entries[i])
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, wallog.WriteEntity(marshaledEntry), "Failed to write entry")
	}

	//important to ensure the entries are flushed into the disk
	assert.NoError(t, wallog.Close(), "Failed to close WAL")

	// recover the data from the WAL
	recoveredEntry, err := wallog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")
	// check if the recovered entries matches with written entries
	for entryIndex, entry := range recoveredEntry {
		unMarshalledEntry := Record{}
		assert.NoError(t, json.Unmarshal(entry.Data, &unMarshalledEntry), "Failed to unmarshal entry")
		// can't use deep equality due the sequence number
		assert.Equal(t, entries[entryIndex].Key, unMarshalledEntry.Key, "Recovered entry does not match written entry (Key)")
		assert.Equal(t, entries[entryIndex].Op, unMarshalledEntry.Op, "Recovered entry does not match written entry (Op)")
		assert.True(t, reflect.DeepEqual(entries[entryIndex].Value, unMarshalledEntry.Value), "Recovered entry does not match written entry (Value)")
	}
}

// Test to verify the repair functionality of the WAL by corrupting the data entry
func TestWAL_WriteRepairRead(t *testing.T) {
	t.Parallel()
	dirPath := "TestWAL_WriteRepairRead.log"
	defer os.RemoveAll(dirPath)
	wallog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to create WAL")

	// write some entries into WAL
	err = wallog.WriteEntity([]byte("entry1"))
	assert.NoError(t, err)
	err = wallog.WriteEntity([]byte("entry2"))
	assert.NoError(t, err)

	// close the WAL
	assert.NoError(t, wallog.Close(), "Failed to close WAL")

	// corrupt the WAL by writing some garbage data
	file, err := os.OpenFile(filepath.Join(dirPath, "segment-0"), os.O_WRONLY|os.O_APPEND, 0644)
	assert.NoError(t, err)
	_, err = file.Write([]byte("garbage data"))
	assert.NoError(t, err)
	file.Close()

	// repair the WAL
	entries, err := wallog.Repair()
	assert.NoError(t, err, "Failed to repair WAL")

	// check if the recovered entries matches with written entries
	assert.Equal(t, 2, len(entries), "Recovered entries count does not match written entries count")
	assert.Equal(t, "entry1", string(entries[0].Data), "Recovered entry does not match written entry")
	assert.Equal(t, "entry2", string(entries[1].Data), "Recovered entry does not match written entry")

	// write some more entries into WAL
	wallog, err = wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to reopen WAL")
	err = wallog.WriteEntity([]byte("entry3"))
	assert.NoError(t, err)

	wallog.Close()

	// recover the data from the WAL
	recoveredEntry, err := wallog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")
	// check if the recovered entries matches with written entries
	assert.Equal(t, 3, len(recoveredEntry), "Recovered entries count does not match written entries count")
	assert.Equal(t, "entry1", string(recoveredEntry[0].Data), "Recovered entry does not match written entry")
	assert.Equal(t, "entry2", string(recoveredEntry[1].Data), "Recovered entry does not match written entry")
	assert.Equal(t, "entry3", string(recoveredEntry[2].Data), "Recovered entry does not match written entry")

}

// test to verify the repair functionality of the WAL by corrupting the CRC
// Similar to previous function, but with a different corruption pattern
// (corrupting the CRC instead of writing random data).
func TestWAL_WriteRepairRead2(t *testing.T) {
	t.Parallel()
	dirPath := "TestWAL_WriteRepairRead2"

	defer os.RemoveAll(dirPath)

	// Create a new WAL
	walog, err := wal.OpenWAL(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err)

	// Write some entries to the WAL
	err = walog.WriteEntity([]byte("entry1"))
	assert.NoError(t, err)
	err = walog.WriteEntity([]byte("entry2"))
	assert.NoError(t, err)

	walog.Close()

	// Corrupt the WAL by writing some random data
	file, err := os.OpenFile(filepath.Join(dirPath, "segment-0"), os.O_WRONLY, 0644)
	assert.NoError(t, err)

	// Read the last entry
	entries, err := walog.ReadAll(false)
	assert.NoError(t, err)
	lastEntry := entries[len(entries)-1]

	// Corrupt the CRC
	lastEntry.CRC = 0
	marshaledEntry := wal.MarshalEntry(lastEntry)

	// Seek to the last entry
	_, err = file.Seek(-int64(len(marshaledEntry)), io.SeekEnd)
	assert.NoError(t, err)

	_, err = file.Write(marshaledEntry)
	assert.NoError(t, err)

	file.Close()

	// Repair the WAL
	entries, err = walog.Repair()
	assert.NoError(t, err)

	// Check that the correct entries were recovered
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, "entry1", string(entries[0].Data))
}
