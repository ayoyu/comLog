package comLog

import (
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var ConfigError = errors.New("Configuration error")
var LogOutOfRange = errors.New("No record exists with this offset")
var LogNotImplemented = errors.New("Not Supported")

/*
Log configuration

	:attr: Data_dir: file system directory where the physical store and index files will be stored
	:attr: NbrOfSegments: (Optional) Number of segments in the existing log data directory (from a second setup)
	:attr: StoreMaxBytes: max bytes to store in the store-file
	:attr: IndexMaxBytes: max bytes to store in the index-file
*/
type Config struct {
	Data_dir      string
	NbrOfSegments int
	StoreMaxBytes uint64
	IndexMaxBytes uint64
}

// The Log struct that holds the list of used segments and maintain the reference
// to the active segment
type Log struct {
	Config
	mu             sync.RWMutex
	segments       []*Segment
	vactiveSegment atomic.Value
}

// Init a new Log instance from the configuration
func NewLog(conf Config) (*Log, error) {
	if conf.Data_dir == "" {
		return nil, ConfigError
	}
	if conf.StoreMaxBytes == 0 || conf.IndexMaxBytes == 0 {
		return nil, ConfigError
	}
	var log *Log = &Log{Config: conf, segments: make([]*Segment, 0, conf.NbrOfSegments)}
	var err error = log.setup()
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
		return err
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
			return err
		}
		// will take baseOffset info only from storeFile
		// the existance of the indexFile that goes with the specific storeFile
		// will be checked later when we initialize the segment
		if strings.HasSuffix(fileInfo.Name(), storeFileSuffix) {
			baseOffsetStr = strings.TrimSuffix(fileInfo.Name(), storeFileSuffix)
			baseOffset, err = strconv.Atoi(baseOffsetStr)
			if err != nil {
				return err
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

		for _, baseoffset := range baseOffsets {
			seg, err = NewSegment(log.Data_dir, log.StoreMaxBytes, log.IndexMaxBytes, baseoffset)
			if err != nil {
				return err
			}
			log.segments = append(log.segments, seg)
		}
	} else {
		// first segment with InitOffset=0
		seg, err = NewSegment(log.Data_dir, log.StoreMaxBytes, log.IndexMaxBytes, 0)
		if err != nil {
			return err
		}
		log.segments = append(log.segments, seg)
	}
	log.segments[len(log.segments)-1].setIsActive(true)
	log.vactiveSegment.Store(log.segments[len(log.segments)-1])
	return nil
}

// Get the active segment
func (log *Log) loadActiveSeg() *Segment {
	return log.vactiveSegment.Load().(*Segment)
}

// Create a new active segment when spliting
func (log *Log) createNewActiveSeg() error {
	var oldSegment *Segment = log.loadActiveSeg()
	oldSegment.setIsActive(false)
	var nextBaseOffset uint64 = oldSegment.getNextOffset()
	var err error
	var newActiveSeg *Segment
	newActiveSeg, err = NewSegment(log.Data_dir, log.StoreMaxBytes, log.IndexMaxBytes, nextBaseOffset)
	if err != nil {
		return err
	}
	log.segments = append(log.segments, newActiveSeg)
	newActiveSeg.setIsActive(true)
	log.vactiveSegment.Store(newActiveSeg)
	return nil
}

// Check if the segment is Full from the index and store files
func (log *Log) splitForNewActiveSeg() bool {
	return log.loadActiveSeg().isFull()
}

// Append a record to the log. It returns the offset, the number of bytes written
// and an error if any
func (log *Log) Append(record []byte) (uint64, int, error) {
	// to ensure split correctness
	log.mu.Lock()
	if log.splitForNewActiveSeg() {
		var err = log.createNewActiveSeg()
		if err != nil {
			log.mu.Unlock()
			return 0, 0, err
		}
	}
	log.mu.Unlock()
	var (
		err    error
		offset uint64
		nn     int
	)
	// Delayed append can happen from a routine that was not able to acquire
	// the activeSegment lock, and so when this happen (lock is acquired) probably
	// the segment that the routine is referencing from previous `loadActiveSeg`
	// is not anymore the active segment (i.e. `log.vactiveSegment.Store` of active segement happened)
	// that's why we should retry in this case to **re-load** the active segment.
	// This behavior occurs during the split segment, because we don't lock the whole append with log mutex
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
	if offset == -1 {
		// last entry -> last segment + last record in the last segment
		return log.segments[len(log.segments)-1]
	}
	var uOffset uint64 = uint64(offset)
	// binary search
	var left int = 0
	var right int = len(log.segments) - 1
	var mid int
	for left <= right {
		mid = left + ((right - left) >> 1)
		// if log.segments[mid]=activeSeg (the last one mid = len(log.segments) - 1)
		// reading the nextOffset at the same time while it's incremented from another goroutine
		// (multiple readers are allowed)
		// TODO: check if the mid is pointing to the activeSeg or not.
		// if not we can read without worying about locking (the Lock implementation in go in this case will CAS
		// and go with the "fast path", so it's worth it to put an If statement instead of executing the CAS operation)
		if uOffset >= log.segments[mid].getNextOffset() {
			left = mid + 1
		} else if uOffset < log.segments[mid].baseOffset {
			right = mid - 1
		} else {
			return log.segments[mid]
		}
	}
	return nil
}

// Read the record corresponding to the given offset. It returns the corresponding record
// the number of bytes read and an error if any
func (log *Log) Read(offset int64) (int, []byte, error) {
	if offset < -1 {
		return 0, nil, LogNotImplemented
	}
	var targetSegment *Segment = log.segmentSearch(offset)
	if targetSegment == nil {
		return 0, nil, LogOutOfRange
	}
	var (
		nn     int
		record []byte
		err    error
	)
	nn, record, err = targetSegment.Read(offset)
	if err != nil {
		return 0, nil, err
	}
	return nn, record, nil
}

// Close the Log. It will close all segemnts it was able to close until an error occur or not.
func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	var err error
	for i := 0; i < len(log.segments); i++ {
		err = log.segments[i].Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Remove the Log. It will remove all segements it was able to remove until an error occur or not
func (log *Log) Remove() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	var err error
	for i := 0; i < len(log.segments); i++ {
		err = log.segments[i].Remove()
		if err != nil {
			return err
		}
	}
	return nil
}
