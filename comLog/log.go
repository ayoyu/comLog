package comLog

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ConfigError       = errors.New("configuration error")
	LogOutOfRange     = errors.New("no record exists with the given offset")
	LogNotImplemented = errors.New("the operation is not supported")
	SetupError        = errors.New("log setup failed, try to fix the issue and run it again")
)

// Log configuration. Be aware that the OS have a limit called "the Operating System File Descriptor Limit" that will constrain
// the maximum number of file descriptors the process has. "Too many open files error" can happen when a process needs
// to open more files than the operating system allows, this limits can constrain how many concurrent requests the server
// can handle. Practically in order to avoid this behavior, you must think of a reasonable `StoreMaxBytes` capacity based
// on the nature of the records you are appending, and also have a background/scheduled thread to run `CollectSegments`
// in order to truncate the the Log based on some offset. Increasing the operating system file descriptor limit can also be an option.
type Config struct {
	// File system directory where the physical store and index files will be stored
	Data_dir string
	// (Optional) Number of segments in the existing log data directory to setup from a second run
	NbrOfSegments int
	// Max bytes to store in the store-file
	StoreMaxBytes uint64
	// Max bytes to store in the index-file
	IndexMaxBytes uint64
}

// The Log structure that holds the list of used segments and maintains the reference to the active segment
type Log struct {
	Config
	mu sync.RWMutex
	// Data segements represented by the store and index
	segments []*Segment
	// The active segment on which the current writes will take place
	vactiveSegment atomic.Value // TODO(performance): check atomic Pointer (*: Store is a bit faster but the Load is not)
}

// Init a new Log instance from the given configuration
func NewLog(conf Config) (*Log, error) {
	if conf.Data_dir == "" {
		return nil, fmt.Errorf("%w: Data_dir is empty", ConfigError)
	}

	if conf.StoreMaxBytes == 0 || conf.IndexMaxBytes == 0 {
		return nil, fmt.Errorf("%w: StoreMaxBytes and IndexMaxBytes cannot be zeros", ConfigError)
	}

	var log *Log = &Log{
		Config:   conf,
		segments: make([]*Segment, 0, conf.NbrOfSegments),
	}
	err := log.setup()
	if err != nil {
		return nil, err
	}

	return log, nil
}

// Setup the log for the first time or from an existing data directory
func (log *Log) setup() error {
	var (
		err     error
		entries []os.DirEntry
	)
	entries, err = os.ReadDir(log.Data_dir)
	if err != nil {
		return fmt.Errorf("%w. Original Err: %w", SetupError, err)
	}

	var (
		fileInfo      os.FileInfo
		baseOffsetStr string
		baseOffset    int
		baseOffsets   []uint64
	)
	for _, entry := range entries {
		fileInfo, err = entry.Info()
		if err != nil {
			return fmt.Errorf("%w. Original Err: %w", SetupError, err)
		}
		// will take baseOffset info only from storeFile
		// the existance of the indexFile that goes with the specific storeFile
		// will be checked later when we initialize the segment
		if strings.HasSuffix(fileInfo.Name(), storeFileSuffix) {
			baseOffsetStr = strings.TrimSuffix(fileInfo.Name(), storeFileSuffix)
			baseOffset, err = strconv.Atoi(baseOffsetStr)
			if err != nil {
				return fmt.Errorf("%w. Original Err: %w", SetupError, err)
			}

			baseOffsets = append(baseOffsets, uint64(baseOffset))
		}

	}

	var seg *Segment
	if len(baseOffsets) > 0 {
		// TODO: check first before sorting if the array is already
		// sorted. Try: https://pkg.go.dev/golang.org/x/exp/slices
		// most of the time the array will be sorted.
		sort.Slice(baseOffsets, func(i, j int) bool {
			return baseOffsets[i] < baseOffsets[j]
		})

		for _, base := range baseOffsets {
			seg, err = NewSegment(log.Data_dir, log.StoreMaxBytes, log.IndexMaxBytes, base)
			if err != nil {
				return fmt.Errorf("%w. Original Err: %w", SetupError, err)
			}

			log.segments = append(log.segments, seg)
		}

	} else {
		// first segment with InitOffset=0
		seg, err = NewSegment(log.Data_dir, log.StoreMaxBytes, log.IndexMaxBytes, 0)
		if err != nil {
			return fmt.Errorf("%w. Original Err: %w", SetupError, err)
		}
		log.segments = append(log.segments, seg)
	}

	log.segments[len(log.segments)-1].setIsActive(true)
	log.vactiveSegment.Store(log.segments[len(log.segments)-1])

	return nil
}

// loadActiveSeg gets the active segment
func (log *Log) loadActiveSeg() *Segment {
	return log.vactiveSegment.Load().(*Segment)
}

// createNewActiveSeg creates a new active segment during the split phase
func (log *Log) createNewActiveSeg() error {
	var oldSegment *Segment = log.loadActiveSeg()
	oldSegment.setIsActive(false)
	// Implicit async flush for the old segment
	err := oldSegment.Flush(INDEX_MMAP_ASYNC)
	if err != nil {
		return err
	}

	var nextBaseOffset uint64 = oldSegment.getNextOffset()
	newActiveSeg, err := NewSegment(log.Data_dir, log.StoreMaxBytes, log.IndexMaxBytes, nextBaseOffset)
	if err != nil {
		return err
	}

	log.segments = append(log.segments, newActiveSeg)
	newActiveSeg.setIsActive(true)
	log.vactiveSegment.Store(newActiveSeg)

	return nil
}

// splitForNewActiveSeg checks if the segment is full from the index and store files
func (log *Log) splitForNewActiveSeg() bool {
	return log.loadActiveSeg().isFull()
}

// Append a record to the log. It returns the offset, the number of bytes written and an error if any
func (log *Log) Append(record []byte) (offset uint64, nn int, err error) {
	// to ensure split correctness
	log.mu.Lock()
	if log.splitForNewActiveSeg() {
		err = log.createNewActiveSeg()
		if err != nil {
			log.mu.Unlock()
			return 0, 0, err
		}
	}
	log.mu.Unlock()

	// Delayed append can happen from a goroutine that was not able to acquire
	// the activeSegment lock, and so when this happen (the lock is acquired) probably
	// the segment that the goroutine is referencing from previous `loadActiveSeg` is not anymore the active
	// segment (i.e. `log.vactiveSegment.Store` of active segement happened).
	// That's why we should retry in this case to **re-load** the active segment.
	// This behavior occurs during the split segment, because we don't lock the whole append with log mutex.
	for {
		// retry
		offset, nn, err = log.loadActiveSeg().Append(record)
		if err != NotActiveAnymore {
			break
		}
	}
	if err != nil {
		return 0, 0, err
	}
	return offset, nn, nil
}

// Search for the corresponding segment given the offset
func (log *Log) segmentSearch(offset int64) *Segment {
	// to protect log.segements slice
	log.mu.RLock()
	defer log.mu.RUnlock()

	currSize := len(log.segments) - 1
	if offset == -1 {
		// offset=-1 means last entry record that will be located in the last segment (aka active segment)
		return log.segments[currSize]
	}

	uOffset := uint64(offset)
	// binary search
	left := 0
	right := len(log.segments) - 1
	var mid int
	for left <= right {
		mid = left + ((right - left) >> 1)
		// check if the mid is pointing to the activeSeg or not. if not we can read without worying
		// about locking. The Lock implementation in go in this case will CAS and go with the "fast path".
		var nextOffset uint64
		if mid == currSize {
			nextOffset = log.segments[mid].getNextOffset()
		} else {
			nextOffset = log.segments[mid].nextOffset
		}

		if uOffset >= nextOffset {
			left = mid + 1
		} else if uOffset < log.segments[mid].baseOffset {
			right = mid - 1
		} else {
			return log.segments[mid]
		}
	}
	return nil
}

// Read reads the record corresponding to the given offset. It returns the corresponding record,
// the number of bytes read and an error if any.
func (log *Log) Read(offset int64) (nn int, record []byte, err error) {
	if offset < -1 {
		return 0, nil, LogNotImplemented
	}

	var targetSegment *Segment = log.segmentSearch(offset)
	if targetSegment == nil {
		return 0, nil, LogOutOfRange
	}

	nn, record, err = targetSegment.Read(offset)
	if err != nil {
		return 0, nil, err
	}
	return nn, record, nil
}

// Explicit Flush/Commit of the log by flushing the active segment (old segments are already flushed to disk).
// The idxSyncType parameter specifies wheter flushing should be done synchronously or asynchronously regarding the index
// mmap linked to the active segment.
func (log *Log) Flush(idxSyncType IndexSyncType) error {
	return log.loadActiveSeg().Flush(idxSyncType)
}

// Close the Log. It will close all segemnts it was able to close until an error occur or not.
func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()

	if len(log.segments) == 0 {
		return nil
	}

	errCh := make(chan error, len(log.segments))
	done := make(chan struct{})
	for i := 0; i < len(log.segments); i++ {
		go func(i int) {
			select {
			case <-done:
				return

			default:
				err := log.segments[i].Close()
				select {
				case <-done:
					return
				case errCh <- err:
				}
			}
		}(i)
	}

	for i := 0; i < len(log.segments); i++ {
		err := <-errCh
		if err != nil {
			// the first occured error is enough to finish and cancel the other goroutines
			close(done)
			return err
		}
	}
	return nil
}

// Remove removes the Log with all its segements it was able to remove until an error occur or not.
func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}

	return os.RemoveAll(log.Data_dir)
}

// SegmentsSize returns the current number of log segments.
func (log *Log) SegmentsSize() int {
	log.mu.RLock()
	defer log.mu.RUnlock()
	return len(log.segments)
}

// LastOffset returns the last tracked offset i.e. the newset offset so far.
func (log *Log) LastOffset() uint64 {
	return log.loadActiveSeg().getNextOffset()
}

// OldestOffset returns the oldest tracked offset so far. If the `CollectSegments` never get triggered or after collecting
// all the segments the oldest offset in this case should be equal 0.
func (log *Log) OldestOffset() uint64 {
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.segments[0].baseOffset
}

// CollectSegments deletes log segements containing records older than the given offset.
// It acts as a segment garbage collector for the log. If there are no segments left, CollectSegments will setup the log again
// so we can have the active segment ready.
func (log *Log) CollectSegments(offset uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	newSegments := make([]*Segment, 0, len(log.segments))

	errCh := make(chan error, len(log.segments))
	done := make(chan struct{})
	delSegs := 0

	for i := 0; i < len(log.segments); i++ {
		if log.segments[i].baseOffset < offset {
			delSegs++
			go func(i int) {
				select {
				case <-done:
					return

				default:
					err := log.segments[i].Remove()
					select {
					case <-done:
						return
					case errCh <- err:
					}
				}
			}(i)

		} else {
			newSegments = append(newSegments, log.segments[i])
		}

	}

	for i := 0; i < delSegs; i++ {
		err := <-errCh
		if err != nil {
			close(done)
			return fmt.Errorf("the collect segments operation failed. "+
				"The log may contain segements pointing to files that no longer exist in the log data directory. "+
				"To recover from this failure, the log must be setup again from the current log data directory: %s. "+
				"Original error: %w", log.Data_dir, err)
		}
	}

	log.segments = newSegments
	if len(log.segments) == 0 {
		// all segments are removed, so we need to setup the log again
		return log.setup()
	}

	return nil
}
