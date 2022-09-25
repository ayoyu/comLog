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
	storeFile, err = newStore(sfile, smaxBytes)
	if err != nil {
		return nil, errors.Wrap(err, seg_context+"Failed to init store file")
	}
	idxfile, err = os.OpenFile(newSeg.getIndexPath(), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, seg_context+"Failed to openFile index file")
	}
	indexFile, err = newIndex(idxfile, idxMaxBytes)
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

func (seg *Segment) isFull() bool {
	// the lock will be at the Log level to check if it creates a new
	// active segment
	return seg.storeFile.size > seg.storeFile.maxBytes || seg.indexFile.size > seg.indexFile.maxBytes
}

func (seg *Segment) Append(record []byte) (int, error) {
	var (
		err        error
		r_position uint64
		nn         int // nbr of bytes written
	)
	seg.mu.Lock()
	defer seg.mu.Unlock()
	nn, r_position, err = seg.storeFile.append(record)
	if err != nil {
		return 0, errors.Wrap(err, seg_context+"Failed to write record in store file")
	}
	err = seg.indexFile.append(seg.nextOffset, r_position)
	if err != nil {
		return 0, errors.Wrap(err, seg_context+"Failed to write to index file")
	}
	seg.nextOffset++
	return nn, nil
}

func (seg *Segment) Read(offset int64) (int, []byte, error) {
	var (
		err          error
		scaledOffset int64
		r_position   uint64
		nn           int // nbr of bytes readed
		record       []byte
	)
	// scale offset to baseOffset
	if offset == -1 {
		scaledOffset = -1 // last entry
	} else {
		scaledOffset = offset - int64(seg.baseOffset)
	}
	seg.mu.RLock()
	defer seg.mu.RUnlock()
	r_position, err = seg.indexFile.read(scaledOffset)
	if err != nil {
		return 0, nil, errors.Wrap(err, seg_context+"Failed to get record position from index file")
	}
	nn, record, err = seg.storeFile.read(r_position)
	if err != nil {
		return 0, nil, errors.Wrap(err, seg_context+"Failed to get record from store file")
	}
	return nn, record, nil
}

func (seg *Segment) Close() error {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	var err error
	err = seg.indexFile.close()
	if err != nil {
		return errors.Wrap(err, seg_context+"Failed to close index file")
	}
	err = seg.storeFile.close()
	if err != nil {
		return errors.Wrap(err, seg_context+"Failed to close store file")
	}
	return nil
}

func (seg *Segment) Remove() error {
	var err error
	err = seg.Close()
	if err != nil {
		return err
	}
	err = os.Remove(seg.storeFile.Name())
	if err != nil {
		return errors.Wrap(err, seg_context+"Failed to remove index file")
	}
	err = os.Remove(seg.indexFile.Name())
	if err != nil {
		return errors.Wrap(err, seg_context+"Failed to remove store file")
	}
	return nil
}

func (seg *Segment) ReadAt(b []byte, position uint64) (int, error) {
	var (
		err error
		nn  int
	)
	seg.mu.RLock()
	defer seg.mu.RUnlock()
	nn, err = seg.storeFile.readAt(b, position)
	if err != nil {
		return 0, errors.Wrap(err, seg_context+"Faild to ReadAt")
	}
	return nn, nil
}
