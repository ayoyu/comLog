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

var ErrNotActiveAnymore = errors.New("abort append, the pointed Segment is not active anymore")

// The Segment structure that holds the pair index-store files.
// It maintains the base Offset and keep track of the next Offset.
type Segment struct {
	// Specifies the first (or base) offset in the segment related to a record.
	// It will be set from the previous segment nextOffset
	baseOffset uint64
	// The log data dir
	path string

	mu    sync.RWMutex
	store *store
	index *index
	// Represents the next offset where the future record can be stored
	nextOffset uint64
	// Indicates wheter a segment is still the active segment or not anymore.
	// Any segment when it get created it becomes the active segment at that time.
	isActive bool
	// Indicates wheter a segment is closed or not for append operations. For read operations the closed signal can
	// be indicated directly from the closed underlying file store, but for append operations (append happens only on the active segment)
	// because we write into memory buffers (store buffer and index mmap) we cannot tell if the segment is closed or not
	// until we hit a read operation (read will flush before reading).
	closedForAppend bool
}

// Create a new segment with the store and index files.
// The `dir` parameter is the file system directory where the physical store and index files will be persisted,
// `stMaxBytes` and `idxMaxBytes`indicate the limit in bytes to store both on the store
// and on the index files, and `baseOffset` is the start indexing offset.
func NewSegment(dir string, stMaxBytes, idxMaxBytes, baseOffset uint64) (*Segment, error) {
	var (
		nextOffset uint64
		err        error

		store *store
		sfile *os.File

		index   *index
		idxfile *os.File
	)

	newSeg := &Segment{
		baseOffset: baseOffset,
		path:       dir,
	}

	sfile, err = os.OpenFile(newSeg.getStorePath(), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf(segContext+"Failed to open the store file. Err: %w", err)
	}
	store, err = newStore(sfile, stMaxBytes)
	if err != nil {
		return nil, fmt.Errorf(segContext+"Failed to init the store. Err: %w", err)
	}

	idxfile, err = os.OpenFile(newSeg.getIndexPath(), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf(segContext+"Failed to open the index file. Err: %w", err)
	}
	index, err = newIndex(idxfile, idxMaxBytes)
	if err != nil {
		return nil, fmt.Errorf(segContext+"Failed to init the index. Err: %w", err)
	}

	nextOffset = index.nbrOfIndexEntries() + baseOffset

	newSeg.store = store
	newSeg.index = index
	newSeg.nextOffset = nextOffset

	return newSeg, nil
}

func (seg *Segment) getStorePath() string {
	return filepath.Join(seg.path, fmt.Sprintf(fileFormat, seg.baseOffset, storeFileSuffix))
}

func (seg *Segment) getIndexPath() string {
	return filepath.Join(seg.path, fmt.Sprintf(fileFormat, seg.baseOffset, indexFileSuffix))
}

// isFull checks if the segment is full
func (seg *Segment) isFull() bool {
	// Even if this is a read operation we choose not to take a `RLock` in order to reduce EOF error appends
	// from the index side when the log split is triggered and a delay append is still pending to append,
	// similar to what can happen in the case of `ErrNotActiveAnymore`.
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
	return seg.store.size >= seg.store.maxBytes ||
		seg.index.maxBytes-seg.index.size < indexEntryWidth
}

// Append a new record to the segment. It returns the offset, number of bytes written and an error if any.
func (seg *Segment) Append(record []byte) (currOffset uint64, nn int, err error) {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.closedForAppend {
		return 0, 0, os.ErrClosed
	}

	if !seg.isActive {
		return 0, 0, ErrNotActiveAnymore
	}

	currOffset = seg.nextOffset

	var pos uint64
	nn, pos, err = seg.store.append(record)
	if err != nil {
		return 0, 0, fmt.Errorf(segContext+"Failed to write record in the store file. Err: %w", err)
	}

	err = seg.index.append(currOffset, pos)
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

// getScaledOffset scales the given offset to the segment `baseOffset`.
func (seg *Segment) getScaledOffset(offset int64) int64 {
	if offset == -1 {
		// The last entry
		return -1
	}

	return offset - int64(seg.baseOffset)
}

// Read reads the record corresponding to the given offset.
// It returns the number of bytes read, the record and an error if any
func (seg *Segment) Read(offset int64) (nn int, record []byte, err error) {
	var (
		scaledOffset          int64
		recordPositionAtStore uint64
	)

	scaledOffset = seg.getScaledOffset(offset)

	seg.mu.RLock()
	defer seg.mu.RUnlock()

	recordPositionAtStore, err = seg.index.read(scaledOffset)
	if err != nil {
		return 0, nil, fmt.Errorf(segContext+"Failed to get record store position from index file. Err: %w", err)
	}

	nn, record, err = seg.store.read(recordPositionAtStore)
	if err != nil {
		return 0, nil, fmt.Errorf(segContext+"Failed to get record from store file. Err: %w", err)
	}

	return nn, record, nil
}

func (seg *Segment) getStoreRecordPosition(offset int64) (uint64, error) {
	scaledOffset := seg.getScaledOffset(offset)

	seg.mu.RLock()
	recordPositionAtStore, err := seg.index.read(scaledOffset)
	seg.mu.RUnlock()

	return recordPositionAtStore, err

}

// Flush/Commit the segment to flush back the store buffer to disk and synchronize synchronously or asynchronously
// the index mmap region with the underlying file.
func (seg *Segment) Flush(idxSyncType IndexSyncType) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()
	var err error

	err = seg.store.buf.Flush()
	if err != nil {
		return fmt.Errorf(segContext+"Failed to flush the store buffer. Err: %w", err)
	}

	switch idxSyncType {
	case INDEX_MMAP_ASYNC:
		err = seg.index.mmap.Sync(gommap.MS_ASYNC)

	case INDEX_MMAP_SYNC:
		err = seg.index.mmap.Sync(gommap.MS_SYNC)

	default:
		return fmt.Errorf("invalid `IndexSyncType` argument, it must be `INDEX_MMAP_SYNC` or INDEX_MMAP_ASYNC")
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

	err := seg.index.close()
	if err != nil {
		return fmt.Errorf(segContext+"Failed to close index file. Err: %w", err)
	}

	err = seg.store.close()
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
		if err = os.Remove(seg.store.name()); err != nil {
			return fmt.Errorf(segContext+"Failed to remove store file %s. Err: %w", seg.store.name(), err)
		}

		if err = os.Remove(seg.index.name()); err != nil {
			return fmt.Errorf(segContext+"Failed to remove index file %s. Err: %w", seg.index.name(), err)
		}
	}

	return err
}

// ReadAt reads from the given position in the store file linked to the segment
// and put it in the given buffer.
// ReadAt is a wrapper arround the store `os.File.ReadAt`.
func (seg *Segment) ReadAt(buf []byte, storePos uint64) (nn int, err error) {
	seg.mu.RLock()
	defer seg.mu.RUnlock()

	nn, err = seg.store.readAt(buf, storePos)
	if err != nil {
		return 0, fmt.Errorf(segContext+"Faild to read from store at position %d. Err: %w", storePos, err)
	}

	return nn, nil
}
