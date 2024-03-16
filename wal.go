package wal

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	syncInterval  = 200 * time.Millisecond // The interval to sync the data from the buffer to the disk.
	segmentPrefix = "segment-"             // The prefix for the segment file.
)

// WAL is a write-ahead log that is used to store the data before it is written to any storage
// It is used to ensure that the data is not lost in case of a crash.
// It is also used to recover the data in case of a crash using automatic recovery.
type WAL struct {
	directory           string             // The directory where the WAL files are stored.
	currentSegment      *os.File           // The current segment file.
	lock                sync.Mutex         // The lock to ensure that the WAL is thread-safe during WAL writes.
	lastSequenceNumber  uint64             // The last sequence number that was used.
	bufWriter           *bufio.Writer      // The buffer writer to write the data to the file.
	syncTimer           *time.Timer        // The sync timer to sync the data from in memory buffer to the disk.
	shouldForceSync     bool               // The flag to force sync the data to the disk.
	maxFileSize         int64              // The maximum log segment size once the segment exceeds the maxFileSize, it creates a new segment.
	maxSegments         int                // The maximum number of segments that can be stored Once the number of segments exceeds the maxSegments, it deletes the oldest segment.
	currentSegmentIndex int                // The index of the latest segment.
	ctx                 context.Context    // The context to help with goroutine management.
	cancel              context.CancelFunc // The cancel to help with goroutine management.
}

/*
OpenWAL creates a new WAL instance if the directory does not exist.
If the directory exists, the last log segment is opened and
the last sequence number is read from the segment file.
enableFsync enables fsync on the log segment file every time the log flushes.
maxFileSize is the maximum size of a log segment file in bytes.
maxSegments is the maximum number of log segment files to keep.
*/
func OpenWAL(directory string, enableFsync bool, maxFileSize int, maxSegments int) (*WAL, error) {
	// create the directory if it does not exist
	if err := os.Mkdir(directory, 0755); err != nil {
		return nil, err
	}

	// get the list of log segment files in the directory
	files, err := filepath.Glob(filepath.Join(directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}
	var lastSegmentId int
	if len(files) > 0 {
		// get the last segment id
		lastSegmentId, err = findLastSegmentId(files)
		if err != nil {
			return nil, err
		}

	} else {
		// incase there are no segment files, create a new segment file
		// create a new segment log file
		segmentFile, err := createSegmentFile(directory, 0)
		if err != nil {
			return nil, err
		}
		// close the segment file
		if err := segmentFile.Close(); err != nil {
			return nil, err
		}
	}

	// open the last segment log file
	lastSegmentLogFilePath := filepath.Join(directory, segmentPrefix+strconv.Itoa(lastSegmentId))
	file, err := os.OpenFile(lastSegmentLogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err

	}
	// seek to the end of the file
	// this is done to ensure that the file is ready for writing
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return nil, err

	}
	ctx, cancel := context.WithCancel(context.Background())
	wal := &WAL{
		directory:           directory,
		currentSegment:      file,
		lastSequenceNumber:  0,
		bufWriter:           bufio.NewWriter(file),
		syncTimer:           time.NewTimer(syncInterval),
		shouldForceSync:     enableFsync,
		maxFileSize:         int64(maxFileSize),
		maxSegments:         maxSegments,
		currentSegmentIndex: lastSegmentId,
		ctx:                 ctx,
		cancel:              cancel,
	}
	return wal, nil
}
