package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
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
	shouldForceSync     bool               // The flag to force sync the data directly to the disk from RAM to Disk to avoid data loss in case of a crash.
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
	if err := os.MkdirAll(directory, 0755); err != nil {
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
	// update the last sequence number from the current segment log file
	updatedLastSegmentId, err := wal.getLastSequenceNumber()
	if err != nil {
		return nil, err
	}
	wal.lastSequenceNumber = uint64(updatedLastSegmentId)

	// sync data from the buffer to the disk
	go wal.keepSyncingData()

	return wal, nil

}

func (w *WAL) keepSyncingData() {
	for {
		select {
		case <-w.syncTimer.C:
			// acquire the lock to ensure that the data is not written to the file while syncing
			w.lock.Lock()
			// sync the data from the buffer to the disk
			err := w.Sync()
			// release the lock
			w.lock.Unlock()
			if err != nil {
				log.Printf("Error while performing sync: %v", err)
			}
		// for graceful shutdown
		case <-w.ctx.Done():
			return
		}
	}
}

// Sync Writes the data from the in-memory buffer to the segment file.
// if fSync is enabled to call the fsync system call in the segment file.
// it resets the syncTimer to sync the data for the next interval.

func (w *WAL) Sync() error {
	// flush the buffer to the segment file
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	if w.shouldForceSync {
		// call the fsync system call to sync the data from the buffer to the disk
		if err := w.currentSegment.Sync(); err != nil {
			return err
		}
	}
	// reset the sync timer
	w.resetSyncTimer()
	return nil
}

// resetSyncTimer resets the syncTimer to sync the data for the next interval.
func (w *WAL) resetSyncTimer() {
	w.syncTimer.Reset(syncInterval)
}

// returns the last sequence number from the current segment file
func (w *WAL) getLastSequenceNumber() (uint64, error) {
	entry, err := w.getLastLogEntry()
	if err != nil {
		return 0, err
	}
	// if the entry is nil, return log sequence number
	if entry == nil {
		return entry.GetLogSequenceNumber(), nil
	}
	return 0, nil
}

// getLastLogEntry iterates through all the entries of the current segment and returns the last entry
func (w *WAL) getLastLogEntry() (*WAL_Entry, error) {
	var (
		previousSize int32
		offset       int64
		entry        *WAL_Entry
	)
	// open the current segment file
	file, err := os.OpenFile(w.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	for {
		var size int32
		// read the size of the entry from the file
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				// reached end of the file, read the last entry at the offset
				if offset == 0 {
					return entry, nil
				}
				// seek to the previous entry offset
				if _, err := file.Seek(offset, io.SeekStart); err != nil {
					return nil, err
				}
				// read the entry data
				data := make([]byte, previousSize)
				if _, err := io.ReadFull(file, data); err != nil {
					return nil, err
				}
				// unmarshal the data into the entity
				entity, err := UnmarshalAndVerifyEntry(data)
				if err != nil {
					return nil, err
				}
				return entity, nil
			}
			return nil, err
		}
		// get the current offset
		offset, err = file.Seek(0, io.SeekCurrent)
		// set the previous size
		previousSize = size
		if err != nil {
			return nil, err
		}
		// skip the next entry
		if _, err := file.Seek(int64(size), io.SeekCurrent); err != nil {
			return nil, err
		}
	}

}

// WriteEntity writes the data to WAL
func (w *WAL) WriteEntity(data []byte) error {
	return w.writeEntity(data, false)
}

// CreateCheckPoint create a check point in the WAL.
// A check point is a special entry in the WAL
// that is used to restore the state of the system to a point where the check point was created.
func (w *WAL) CreateCheckPoint(data []byte) error {
	w.writeEntity(data, true)
	return nil
}

// writeEntity writes the wal entity to the buffWriter and
// sync the data to disk if the entity is of type check point
func (w *WAL) writeEntity(data []byte, isCheckpoint bool) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	// rotate the segment file if the current segment file exceeds the maxFileSize
	if err := w.rotateLogIfRequired(); err != nil {
		return err
	}
	// increment the sequence number
	w.lastSequenceNumber++
	// create a wal entry
	entry := &WAL_Entry{
		LogSequenceNumber: w.lastSequenceNumber,
		Data:              data,
		CRC:               crc32.ChecksumIEEE(append(data, byte(w.lastSequenceNumber))), // calculate the CRC of the data and the sequence number to verify the data integrity
	}
	if isCheckpoint {
		if err := w.Sync(); err != nil {
			return fmt.Errorf("could not create check point, error while syncing data: %v", err)
		}
		entry.IsCheckPoint = &isCheckpoint
	}
	// write the entry to the buffer
	return w.writeEntryToBuffer(entry)

}

func (w *WAL) rotateLogIfRequired() error {
	// stat the current segment file
	fileInfo, err := w.currentSegment.Stat()
	if err != nil {
		return err
	}
	// if the current segment file plus the buffer writer size exceeds the maxFileSize
	//NOTE: here we include the buffer writer size is the size of the data that is yet to be written to the disk
	if fileInfo.Size()+int64(w.bufWriter.Buffered()) >= w.maxFileSize {
		// rotate the log
		if err := w.rotateLog(); err != nil {
			return err
		}
	}
	return nil
}

// rotateLog rotates the log by creating a new segment file
func (w *WAL) rotateLog() error {
	// sync the data from the buffer to the disk before rotating the log
	if err := w.Sync(); err != nil {
		return err
	}
	// close the current segment file
	if err := w.currentSegment.Close(); err != nil {
		return err
	}
	// increment the segment index
	w.currentSegmentIndex++
	// check if current segment index exceeds the maxSegments
	if w.currentSegmentIndex >= w.maxSegments {
		// delete the oldest segment file
		if err := w.deleteOldestSegment(); err != nil {
			return err
		}
	}
	// create a new segment file
	segmentFile, err := createSegmentFile(w.directory, w.currentSegmentIndex)
	if err != nil {
		return err
	}
	// update the current segment file
	w.currentSegment = segmentFile
	// update the buffer writer
	w.bufWriter = bufio.NewWriter(segmentFile)
	return nil

}

// deleteOldestSegment deletes the oldest segment file
func (w *WAL) deleteOldestSegment() error {
	var oldestSegmentFilePath string
	// get the list of log segment files with the segment prefix in the directory
	files, err := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil

	} else {
		// find the oldest segment id
		oldestSegmentFilePath, err = w.getOldestSegmentFile(files)
		if err != nil {
			return err
		}
	}
	// delete the oldest segment file
	if err := os.Remove(oldestSegmentFilePath); err != nil {
		return err
	}
	return nil
}

// getOldestSegmentFile returns the path of the oldest segment file from the list of files
func (w *WAL) getOldestSegmentFile(files []string) (string, error) {
	var oldestSegmentFilePath string
	oldestSegmentId := math.MaxInt64 // initialize the oldest segment id to the maximum integer value
	for _, file := range files {
		// get the segment id from the file name by removing the prefix and converting it to an integer
		segmentId, err := strconv.Atoi(strings.TrimPrefix(file,
			filepath.Join(w.directory, segmentPrefix)))
		if err != nil {
			return "", err
		}
		// if the segment id is less than the oldest segment id, update the oldest segment id
		if segmentId < oldestSegmentId {
			oldestSegmentId = segmentId
			oldestSegmentFilePath = file
		}
	}
	return oldestSegmentFilePath, nil

}

// Close closes the WAL, also calls sync to sync the data from the buffer to the disk
func (w *WAL) Close() error {
	// cancel the context
	w.cancel()
	if err := w.Sync(); err != nil {
		return err
	}
	// close the current segment file
	return w.currentSegment.Close()
}

func (w *WAL) writeEntryToBuffer(entry *WAL_Entry) error {
	// marshal the entity before writing it to the buffer
	marshalEntry := MarshalEntry(entry)
	// size of the entry
	size := int32(len(marshalEntry))
	// write the size of the entry to the buffer before writing the entry to the buffer
	// this is done to read the size of the entry before reading the entry
	if err := binary.Write(w.bufWriter, binary.LittleEndian, size); err != nil {
		return err
	}
	// write the entry to the buffer
	if _, err := w.bufWriter.Write(marshalEntry); err != nil {
		return err
	}
	return nil
}

// Read Only Operations

// ReadAll reads all the entries from the WAL. if readFromCheckPoint is true, it reads from the check point.
// it will return all the entries from last checkpoint (if no checkpoint is found)
// if readFromCheckPoint is false, it reads from the beginning of the WAL.
func (w *WAL) ReadAll(readFromCheckPoint bool) ([]*WAL_Entry, error) {
	// open the current segment file
	file, err := os.OpenFile(w.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// read all the entries from the current segment file
	entries, checkPoint, err := w.ReadAllEntriesFromFile(file, readFromCheckPoint)
	if err != nil {
		return entries, err
	}
	// if the readFromCheckPoint is true and the check point is not found
	if readFromCheckPoint && checkPoint <= 0 {
		// return an empty slice
		return entries[:0], nil
	}
	return entries, nil
}

// ReadAllFromOffset start reading all log segment files from the given offset.
// (segment Index) and returns all the entries.
// If readFromCheckpoint is true, it will return all the entries from the last checkpoint (if no checkpoint is
// found, it will return an empty slice.)
func (w *WAL) ReadAllFromOffset(offset int, readFromCheckPoint bool) ([]*WAL_Entry, error) {
	// get the list of log segment files in the directory
	files, err := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}
	var entries []*WAL_Entry
	prevCheckPointLogSequenceNumber := uint64(0) // initialize the previous check point log sequence number to 0
	for _, file := range files {
		// get the segment index from the file name
		segmentIndex, err := strconv.Atoi(strings.TrimPrefix(file, filepath.Join(w.directory, segmentPrefix)))
		if err != nil {
			return nil, err
		}
		// if the segment index is less than the offset, skip the segment
		if segmentIndex < offset {
			continue
		}
		// open the segment file, here all the segment file index is greater than the offset
		segmentFile, err := os.OpenFile(file, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}
		entriesFromSegmentFile, checkpoint, err := w.ReadAllEntriesFromFile(segmentFile, readFromCheckPoint)
		if err != nil {
			return nil, err
		}
		// if the checkpoint is greater than the previous checkpoint log sequence number
		if readFromCheckPoint && checkpoint > prevCheckPointLogSequenceNumber {
			// clear the entries
			entries = entries[:0]
			// update the previous checkpoint log sequence number
			prevCheckPointLogSequenceNumber = checkpoint
		}
		// append the entries from the segment file to the entries
		entries = append(entries, entriesFromSegmentFile...)
	}
	return entries, nil
}

// ReadAllEntriesFromFile reads all the entries from the given file.
// returns all the entries from the file and the last checkpoint log sequence number.
func (w *WAL) ReadAllEntriesFromFile(file *os.File, readFromCheckPoint bool) ([]*WAL_Entry, uint64, error) {
	var entries []*WAL_Entry
	checkPointLogSequenceNumber := uint64(0)
	for {
		var size int32
		// read the size of the entry from the file
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			// when the end of the file is reached, break the loop
			if err == io.EOF {
				break
			}
			return entries, checkPointLogSequenceNumber, err
		}
		data := make([]byte, size)
		// read the data from the file
		_, err := io.ReadFull(file, data)
		if err != nil {
			return entries, checkPointLogSequenceNumber, err
		}
		// unmarshal the data into the entity
		entity, err := UnmarshalAndVerifyEntry(data)
		if err != nil {
			return entries, checkPointLogSequenceNumber, err
		}
		// if we are reading the entries from the checkpoint
		// and we find the checkpoint entity, we should return
		// the entries from the last checkpoint. So we empty the entries
		// and start appending the entries from the checkpoint.
		if entity.IsCheckPoint != nil && entity.GetIsCheckPoint() {
			checkPointLogSequenceNumber = entity.GetLogSequenceNumber()
			// reset the entries to read from the checkpoint
			entries = entries[:0]
		}
		// append the entity to the entries
		entries = append(entries, entity)
	}
	return entries, checkPointLogSequenceNumber, nil
}

// Repair repairs the corrupted WAL by scanning the WAL from the start
// and read all entries until a corrupted entry is encountered, at that point
// the file is truncated. The func returns the number of entries that were read
// before the corruption and overwrites the existing WAL files with the repaired entries.
// For the WAL corruption, it verifies the CRC of the entry, if the CRC is not valid, it truncates the file at that point.

func (w *WAL) Repair() ([]*WAL_Entry, error) {
	// get all the segment files in the directory
	files, err := filepath.Glob(filepath.Join(w.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}
	var lastSegmentId int
	if len(files) == 0 {
		log.Fatalf("No segment files found in the directory: %s, nothing to repair", w.directory)
	} else {
		// get the last segment id
		lastSegmentId, err = findLastSegmentId(files)
		if err != nil {
			return nil, err
		}
	}
	// open the last segment log file
	lastSegmentLogFilePath := filepath.Join(w.directory, segmentPrefix+strconv.Itoa(lastSegmentId))
	file, err := os.OpenFile(lastSegmentLogFilePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// seek to the beginning of the file
	// this is done to ensure that the file is ready for reading
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var entries []*WAL_Entry
	for {
		// read the size of the next entry from the file
		var size int32
		if err := binary.Read(file, binary.LittleEndian, &size); err != nil {
			// if the end of the file is reached, no corruption is found
			if err == io.EOF {
				return entries, nil
			}
			// Error while reading the size of the entry
			log.Printf("Error while reading the size of the entry: %v", err)
			// truncate the file at this point
			if err := w.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return nil, nil
		}
		// read the entry data from the file
		data := make([]byte, size)
		if _, err := io.ReadFull(file, data); err != nil {
			// truncate the file at this point
			if err := w.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return entries, nil
		}

		// unmarshal the data into the entity
		var entry WAL_Entry
		// skip the check point entry

		if entry.IsCheckPoint != nil && entry.GetIsCheckPoint() {
			log.Printf("Skipping the check point entry")
			continue
		}
		if err := proto.Unmarshal(data, &entry); err != nil {
			// Error while unmarshalling the data

			log.Printf("Error while unmarshalling the data: %v", err)
			// truncate the file at this point
			if err := w.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return entries, nil
		}
		// verify the CRC of the entry
		if !VerifyCRC(&entry) {
			// CRC mismatch, truncate the file at this point
			log.Printf("CRC mismatch: data may be corrupted")
			if err := w.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return entries, nil
		}
		// append the entry to the entries
		entries = append(entries, &entry)
	}
}

// replaceWithFixedFile replaces the existing WAL file with the given entries atomically.
func (w *WAL) replaceWithFixedFile(entries []*WAL_Entry) error {
	// create a temporary file to makes the repair atomic
	tempFilePath := fmt.Sprintf("%s.tmp", w.currentSegment.Name())
	tempFile, err := os.OpenFile(tempFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	// write the entries to the temporary file
	for _, entry := range entries {
		// marshal the entity before writing it to the file
		marshalEntry := MarshalEntry(entry)
		// size of the entry
		size := int32(len(marshalEntry))
		// write the size of the entry to the file before writing the entry to the file
		// this is done to read the size of the entry before reading the entry
		if err := binary.Write(tempFile, binary.LittleEndian, size); err != nil {
			return err
		}
		// write the entry to the file
		if _, err := tempFile.Write(marshalEntry); err != nil {
			return err
		}
	}
	// close the temporary file
	if err := tempFile.Close(); err != nil {
		return err
	}
	// rename the temporary file to the original file
	if err := os.Rename(tempFilePath, w.currentSegment.Name()); err != nil {
		return err
	}
	return nil
}
