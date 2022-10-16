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

type Config struct {
	Data_dir      string
	NbrOfSegments int
	StoreMaxBytes uint64
	IndexMaxBytes uint64
}

type Log struct {
	Config
	mu             sync.RWMutex
	segments       []*Segment
	vactiveSegment atomic.Value
}

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

func (log *Log) loadActiveSeg() *Segment {
	return log.vactiveSegment.Load().(*Segment)
}

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

func (log *Log) splitForNewActiveSeg() bool {
	return log.loadActiveSeg().isFull()
}

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
	// is not anymore the active segment (i.e. `log.vactiveSegment.Store` of active segement happenned)
	// that's why we should retry in this case to **re-load** the active segment.
	// This behavior occur during the split segment, because we don't lock the whole append with log mutex
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
		// TODO: this can be an issue if log.segments[mid]=activeSeg (last one)
		// when you read the nextOffset it can be written by another append
		if uOffset >= log.segments[mid].nextOffset {
			left = mid + 1
		} else if uOffset < log.segments[mid].baseOffset {
			right = mid - 1
		} else {
			return log.segments[mid]
		}
	}
	return nil
}

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

func (log *Log) Close() error {
	// will close all segemnts it was able to close until an error
	// occur or not
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

func (log *Log) Remove() error {
	// will remove all segements it was able to remove until an error
	// occur or not
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
