package wal

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

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
