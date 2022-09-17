package comLog

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

const (
	storeFileSuffix = ".store"
	indexFileSuffix = ".index"
	fileFormat      = "%d%s"
	seg_context     = "[Segment]: "
)

type Segment struct {
	mu         sync.RWMutex
	storeFile  *store
	indexFile  *index
	baseOffset uint64 // will be set from the previous segment nextOffset
	nextOffset uint64
	path       string
}

func NewSegment(segment_dir string, smaxBytes, idxMaxBytes uint64, baseOffset uint64) (*Segment, error) {
	var (
		err        error
		storeFile  *store
		sfile      *os.File
		indexFile  *index
		idxfile    *os.File
		nextOffset uint64
	)
	var newSeg *Segment = &Segment{baseOffset: baseOffset, path: segment_dir}
	sfile, err = os.OpenFile(newSeg.getStorePath(), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, seg_context+"Failed to openFile store file")
	}
	storeFile, err = NewStore(sfile, smaxBytes)
	if err != nil {
		return nil, errors.Wrap(err, seg_context+"Failed to init store file")
	}
	idxfile, err = os.OpenFile(newSeg.getIndexPath(), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, seg_context+"Failed to openFile index file")
	}
	indexFile, err = NewIndex(idxfile, idxMaxBytes)
	if err != nil {
		return nil, errors.Wrap(err, seg_context+"Failed to init index file")
	}
	// this will generate also for existing files with some size
	// if size=0 this will be just the baseOffset
	nextOffset = indexFile.nbrOfIndexes() + baseOffset

	newSeg.storeFile = storeFile
	newSeg.indexFile = indexFile
	newSeg.nextOffset = nextOffset
	return newSeg, nil
}

func (seg *Segment) getStorePath() string {
	// startOffset.store
	return filepath.Join(seg.path, fmt.Sprintf(fileFormat, seg.baseOffset, storeFileSuffix))
}

func (seg *Segment) getIndexPath() string {
	return filepath.Join(seg.path, fmt.Sprintf(fileFormat, seg.baseOffset, indexFileSuffix))
}

func (seg *Segment) IsFull() bool {
	return seg.storeFile.size > seg.storeFile.maxBytes || seg.indexFile.size > seg.indexFile.maxBytes
}
