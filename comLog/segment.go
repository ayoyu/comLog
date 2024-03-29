package comLog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/tysonmote/gommap"
)

const (
	storeFileSuffix = ".store"
	indexFileSuffix = ".index"
	fileFormat      = "%d%s"
	segContext      = "[segment]: "
)

type IndexSyncType uint8

const (
	INDEX_MMAP_SYNC IndexSyncType = iota
	INDEX_MMAP_ASYNC
)

var NotActiveAnymore = errors.New("abort append, the pointed Segment is not active anymore")

// The Segment structure that holds the pair index-store files.
// It maintains the base Offset and keep track of the next Offset.
type Segment struct {
	mu        sync.RWMutex
	storeFile *store
	indexFile *index
	// Specifies the first (or base) offset in the segment related to a record.
	// It will be set from the previous segment nextOffset
	baseOffset uint64
	// Represents the next offset where the future record can be stored
	nextOffset uint64
	// The log data dir
	path string
	// Indicates wheter a segment is still the active segment or not anymore. Any segment when it get created
	// it will active at the current time.
	isActive bool
	// Indicates wheter a segment is closed or not for append operations. For read operations the closed signal can
	// be indicated directly from the closed underlying file store, but for append operations (append happens only on the active segment)
	// because we write into memory buffers (store buffer and index mmap) we cannot tell if the segment is closed or not
	// until we hit a read operation (read will flush before reading).
	closedForAppend bool
}

// Create a new segment with the store and index files. The `dir` parameter is the file system directory where the physical
// store and index files will be persisted, `stMaxBytes` and `idxMaxBytes`indicate the limit in bytes to store both on the store
// and on the index files and `baseOffset` is the start indexing offset.
func NewSegment(dir string, stMaxBytes, idxMaxBytes, baseOffset uint64) (*Segment, error) {
	var (
		err        error
		storeFile  *store
		sfile      *os.File
		indexFile  *index
		idxfile    *os.File
		nextOffset uint64
	)

	var newSeg *Segment = &Segment{
		baseOffset: baseOffset,
		path:       dir,
	}
	// init the store
	sfile, err = os.OpenFile(newSeg.getStorePath(), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf(segContext+"Failed to open the store file. Err: %w", err)
	}
	storeFile, err = newStore(sfile, stMaxBytes)
	if err != nil {
		return nil, fmt.Errorf(segContext+"Failed to init the store. Err: %w", err)
	}

	// init the index
	idxfile, err = os.OpenFile(newSeg.getIndexPath(), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf(segContext+"Failed to open the index file. Err: %w", err)
	}
	indexFile, err = newIndex(idxfile, idxMaxBytes)
	if err != nil {
		return nil, fmt.Errorf(segContext+"Failed to init the index. Err: %w", err)
	}
	// this will generalize also for existing files with a certain size
	// (if size=0 this will be just the baseOffset)
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
	// Even if this is a read operation we choose not to take a `RLock` in order to reduce EOF error appends
	// from the index side when the log split is triggered and a delay append is still pending to append,
	// similar to what can happen in the case of `NotActiveAnymore`.
	// We could do the same trick by retrying (similar to the CAS operation), but after benchmarking we figure out
	// it's not worth it (no clear gain at all and we will just add another complexity).
	seg.mu.Lock()
	defer seg.mu.Unlock()
	// To reduce missing appends:
	// it's better to check the index full situation while adding indexWidth(=16)
	// to not hit a lot of missing appends and wait for the store to grow with missing
	// appends (i.e appends without indexing) to trigger the `isFull` from the store side
	// an example: index.size=560(=16 * 35) while index.maxByte = 563, in this situation
	// it will wait until the store.size trigger the maxed with missing appends
	// (the index file contains fixed sequence of byte of length 16)
	return seg.storeFile.size >= seg.storeFile.maxBytes ||
		seg.indexFile.maxBytes-seg.indexFile.size < indexWidth
}

// Append a new record to the segment.
// It returns the offset, number of bytes written and an error if any
func (seg *Segment) Append(record []byte) (currOffset uint64, nn int, err error) {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.closedForAppend {
		return 0, 0, os.ErrClosed
	}

	if !seg.isActive {
		return 0, 0, NotActiveAnymore
	}

	currOffset = seg.nextOffset
	var pos uint64
	nn, pos, err = seg.storeFile.append(record)
	if err != nil {
		return 0, 0, fmt.Errorf(segContext+"Failed to write record in the store file. Err: %w", err)
	}

	err = seg.indexFile.append(currOffset, pos)
	if err != nil {
		return 0, 0, fmt.Errorf(segContext+"Failed to write to index file. Err: %w", err)
	}

	seg.nextOffset++
	return currOffset, nn, nil
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

// Read reads the record corresponding to the given offset.
// It returns the number of bytes read, the record and an error if any
func (seg *Segment) Read(offset int64) (nn int, record []byte, err error) {
	var (
		scaledOffset int64
		pos          uint64
	)
	// scale the offset to baseOffset
	if offset == -1 {
		scaledOffset = -1 // last entry
	} else {
		scaledOffset = offset - int64(seg.baseOffset)
	}

	seg.mu.RLock()
	defer seg.mu.RUnlock()
	pos, err = seg.indexFile.read(scaledOffset)
	if err != nil {
		return 0, nil, fmt.Errorf(segContext+"Failed to get record position from index file. Err: %w", err)
	}

	nn, record, err = seg.storeFile.read(pos)
	if err != nil {
		return 0, nil, fmt.Errorf(segContext+"Failed to get record from store file. Err: %w", err)
	}

	return nn, record, nil
}

// Flush/Commit the segment to flush back the store buffer to disk and synchronize synchronously or asynchronously
// the index mmap region with the underlying file.
func (seg *Segment) Flush(idxSyncType IndexSyncType) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	var err error
	err = seg.storeFile.writeBuf.Flush()
	if err != nil {
		return fmt.Errorf(segContext+"Failed to flush the store buffer. Err: %w", err)
	}

	switch idxSyncType {
	case INDEX_MMAP_ASYNC:
		err = seg.indexFile.mmap.Sync(gommap.MS_ASYNC)

	case INDEX_MMAP_SYNC:
		err = seg.indexFile.mmap.Sync(gommap.MS_SYNC)
	}

	if err != nil {
		return fmt.Errorf(segContext+"Failed to sync the index mmap. Err: %w", err)
	}
	return nil
}

// Close the segment by closing the store and index files
func (seg *Segment) Close() error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	seg.closedForAppend = true

	err := seg.indexFile.close()
	if err != nil {
		return fmt.Errorf(segContext+"Failed to close index file. Err: %w", err)
	}

	err = seg.storeFile.close()
	if err != nil {
		return fmt.Errorf(segContext+"Failed to close store file. Err: %w", err)
	}

	return nil
}

// Remove the segment by removing the store and index files
func (seg *Segment) Remove() error {
	// After closing the segment any pending IO operation will be canceled and return immediatly with an os.ErrClosed error.
	// That's why we release the lock at this stage to let the other IO operations/goroutines (read, append,...)
	// to get immediatly their responses without any delay. The response can be also of type os.PathError error if files are removed
	// before the IO operations get to be applied.
	err := seg.Close()

	seg.mu.Lock()
	defer seg.mu.Unlock()
	if err == nil {
		if err = os.Remove(seg.storeFile.name()); err != nil {
			return fmt.Errorf(segContext+"Failed to remove store file %s. Err: %w", seg.storeFile.name(), err)
		}
		if err = os.Remove(seg.indexFile.name()); err != nil {
			return fmt.Errorf(segContext+"Failed to remove index file %s. Err: %w", seg.indexFile.name(), err)
		}
	}

	return err
}

// ReadAt reads from the given position in the store file linked to the segment and put it in the given buffer.
func (seg *Segment) ReadAt(buf []byte, position uint64) (nn int, err error) {
	seg.mu.RLock()
	defer seg.mu.RUnlock()

	nn, err = seg.storeFile.readAt(buf, position)
	if err != nil {
		return 0, fmt.Errorf(segContext+"Faild to read at position %d. Err: %w", position, err)
	}

	return nn, nil
}
