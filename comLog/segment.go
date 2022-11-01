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

var NotActiveAnymore = errors.New("Abort append, the Segment becomes old")

// The Segment structure that holds the pair index-store files.
// It maintains the base Offset and keep track of the next Offset
type Segment struct {
	mu         sync.RWMutex
	storeFile  *store
	indexFile  *index
	baseOffset uint64 // will be set from the previous segment nextOffset
	nextOffset uint64
	path       string
	isActive   bool
}

/*
Init a new segment (store and index files)

	:param: dir: file system directory where the physical store and index files will be stored
	:param: smaxBytes: max bytes to store in the store-file
	:param: idxMaxBytes: max bytes to store in the index-file
	:param: baseOffset: the start indexing offset
*/
func NewSegment(dir string, smaxBytes, idxMaxBytes uint64, baseOffset uint64) (*Segment, error) {
	var (
		err        error
		storeFile  *store
		sfile      *os.File
		indexFile  *index
		idxfile    *os.File
		nextOffset uint64
	)
	var newSeg *Segment = &Segment{baseOffset: baseOffset, path: dir}
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
	// this will generalize also for existing files with some size
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

// check if segment is full
func (seg *Segment) isFull() bool {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	// To reduce missing appends:
	// it's better to check the index full situation while adding indexWidth(=16)
	// to not hit a lot of missing appends and wait for the store to grow with missing
	// appends (i.e appends without indexing) to trigger the `isFull` from the store side
	// an example: index.size=560(=16 * 35) while index.maxByte = 563, in this situation
	// it will wait until the store.size trigger the maxed with missing appends
	// (the index file contains fixed sequence of byte of length 16)
	return seg.storeFile.size >= seg.storeFile.maxBytes || seg.indexFile.size+indexWidth >= seg.indexFile.maxBytes
}

// Append a new record to the segment.
// It returns the offset, number of bytes written and an error if any
func (seg *Segment) Append(record []byte) (uint64, int, error) {
	var (
		err        error
		r_position uint64
		nn         int // nbr of bytes written
	)
	seg.mu.Lock()
	defer seg.mu.Unlock()
	if !seg.isActive {
		return 0, 0, NotActiveAnymore
	}
	var curr_offset uint64 = seg.nextOffset
	nn, r_position, err = seg.storeFile.append(record)
	if err != nil {
		return 0, 0, errors.Wrap(err, seg_context+"Failed to write record in store file")
	}
	err = seg.indexFile.append(curr_offset, r_position)
	if err != nil {
		return 0, 0, errors.Wrap(err, seg_context+"Failed to write to index file")
	}
	seg.nextOffset++
	return curr_offset, nn, nil
}

func (seg *Segment) getNextOffset() uint64 {
	seg.mu.RLock()
	defer seg.mu.RUnlock()
	return seg.nextOffset
}

func (seg *Segment) setIsActive(b bool) {
	seg.mu.Lock()
	seg.isActive = b
	seg.mu.Unlock()
}

// Get record corresponding to the given offset.
// It returns the number of bytes read, the record and an error if any
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

// Close the segment by closing the store and index files
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

// Remove the segment by removing the store and index files
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

// Read at the corresponding position in the store file linked to the segment
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
