package tests

import (
	"encoding/json"
	"os"
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

	// test data to write
	entries := []Record{
		{Key: "key1", Value: []byte("value1"), Op: InsertOperation},
		{Key: "key2", Value: []byte("value2"), Op: InsertOperation},
		{Key: "key3", Op: DeleteOperation},
	}
	// write the test data to wal
	for _, entry := range entries {
		// marshal the record before writing
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntity(marshaledEntry), "Failed to write entry")
	}
}
