package comLog

import (
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	mu            sync.RWMutex
	segments      []*Segment
	activeSegment *Segment
}

func NewLog(conf Config) (*Log, error) {
	if conf.Data_dir == "" {
		return nil, ConfigError
	}
	if conf.StoreMaxBytes == 0 || conf.IndexMaxBytes == 0 {
		return nil, ConfigError
	}
	var log *Log = &Log{Config: conf, segments: make([]*Segment, 0, conf.NbrOfSegments), activeSegment: nil}
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
			seg, err = log.createNewSegment(baseoffset, false)
			if err != nil {
				return err
			}
			log.segments = append(log.segments, seg)
		}
		log.activeSegment = log.segments[len(log.segments)-1]
	} else {
		// first segment with InitOffset=0
		seg, err = log.createNewSegment(0, true)
		if err != nil {
			return err
		}
		log.segments = append(log.segments, seg)
	}
	return nil
}

func (log *Log) createNewSegment(baseoffset uint64, setActive bool) (*Segment, error) {
	var (
		seg *Segment
		err error
	)
	seg, err = NewSegment(log.Data_dir, log.StoreMaxBytes, log.IndexMaxBytes, baseoffset)
	if err != nil {
		return nil, err
	}
	if setActive {
		log.activeSegment = seg
	}
	return seg, nil
}

func (log *Log) checkActiveSegment() error {
	// check if new active segment must be created
	var err error
	var newSeg *Segment
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.activeSegment.isFull() {
		var nextBaseOffset uint64 = log.activeSegment.nextOffset
		newSeg, err = log.createNewSegment(nextBaseOffset, true)
		if err != nil {
			return err
		}
		log.segments = append(log.segments, newSeg)
	}
	return nil
}

func (log *Log) Append(record []byte) (uint64, int, error) {
	var err error
	err = log.checkActiveSegment()
	if err != nil {
		return 0, 0, err
	}
	var (
		offset uint64
		nn     int
	)
	offset, nn, err = log.activeSegment.Append(record)
	if err != nil {
		return 0, 0, err
	}
	return offset, nn, nil
}

func (log *Log) segmentSearch(offset int64) *Segment {
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
