package wal

import (
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var ErrCRCMismatch = errors.New("CRC mismatch: data may be corrupted")

// find the last segment id from the list of files
func findLastSegmentId(files []string) (int, error) {
	var lastSegmentId int
	for _, file := range files {
		_, fileName := filepath.Split(file)
		// get the segment id from the file name by removing the prefix
		segmentId, err := strconv.Atoi(strings.TrimPrefix(fileName, segmentPrefix))
		if err != nil {
			return 0, err
		}
		// if the segment id is greater than the last segment id, update the last segment id
		if segmentId > lastSegmentId {
			lastSegmentId = segmentId
		}
	}
	return lastSegmentId, nil
}

// create the segment log file with the given segmentId in the given directory
func createSegmentFile(directory string, segmentId int) (*os.File, error) {
	// create the segment file
	segmentFile, err := os.Create(filepath.Join(directory, segmentPrefix+strconv.Itoa(segmentId)))
	if err != nil {
		return nil, err
	}
	return segmentFile, nil
}

// UnmarshalAndVerifyEntry unmarshals the data into a WAL_Entry and verifies the CRC.
func UnmarshalAndVerifyEntry(data []byte) (*WAL_Entry, error) {
	var entity WAL_Entry
	// unmarshal the data into the entity
	UnmarshalEntry(data, &entity)
	// verify the CRC
	if !VerifyCRC(&entity) {
		return nil, ErrCRCMismatch
	}
	return &entity, nil

}

// VerifyCRC verifies weather the given entity has the correct CRC.
func VerifyCRC(entity *WAL_Entry) bool {
	// calculate the CRC of the entity
	actualCRC := calculateCRC(entity)
	// compare the calculated CRC with the entity's CRC
	return entity.CRC == actualCRC
}

// calculateCRC calculates the CRC of the given entity.
func calculateCRC(entity *WAL_Entry) uint32 {
	return crc32.ChecksumIEEE(append(entity.GetData(), byte(entity.GetLogSequenceNumber())))
}
