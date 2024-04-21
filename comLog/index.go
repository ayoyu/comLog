package comLog

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offsetWidth     = 8
	positionWidth   = 8
	indexEntryWidth = offsetWidth + positionWidth
	indexContext    = "[index]: "
)

var ErrIndexOutOfRange = errors.New("no index exists with the given offset")

type index struct {
	file     *os.File
	size     uint64
	mmap     gommap.MMap
	maxBytes uint64
}

func newIndex(file *os.File, maxBytes uint64) (*index, error) {
	var (
		err      error
		mmap     gommap.MMap
		fileInfo os.FileInfo
	)

	fileInfo, err = os.Stat(file.Name())
	if err != nil {
		return nil, fmt.Errorf(indexContext+"failed to get file stat for file %s: %w", file.Name(), err)
	}

	// Real size before growing the file index
	realFileSize := uint64(fileInfo.Size())
	// Grow the size of the file to get the mmap-buf with the maxBytes size
	err = os.Truncate(file.Name(), int64(maxBytes))
	if err != nil {
		return nil, fmt.Errorf(indexContext+"failed to truncate the index file %s to grow its size to maxBytes: %w", file.Name(), err)
	}

	mmap, err = gommap.Map(file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf(indexContext+"failed to get mmap for the index file %s: %w", file.Name(), err)
	}

	return &index{
		file:     file,
		size:     realFileSize,
		mmap:     mmap,
		maxBytes: maxBytes,
	}, nil
}

func (idx *index) append(offset, position uint64) error {
	currPos := idx.size

	if idx.maxBytes-currPos < indexEntryWidth {
		return fmt.Errorf(indexContext+"no more space to append the (offset,position) tuple: %w", io.EOF)
	}

	encoding.PutUint64(idx.mmap[currPos:currPos+offsetWidth], offset)
	encoding.PutUint64(idx.mmap[currPos+offsetWidth:currPos+indexEntryWidth], position)
	idx.size += indexEntryWidth

	return nil
}

func (idx *index) read(offset int64) (uint64, error) {
	var pos uint64
	if offset == -1 {
		// last entry
		pos = idx.size - indexEntryWidth
	} else {
		pos = uint64(offset) * indexEntryWidth
	}

	if pos+indexEntryWidth > idx.size {
		// this pos is not yet filled
		return 0, ErrIndexOutOfRange
	}

	recordPositionAtStore := encoding.Uint64(idx.mmap[pos+offsetWidth : pos+indexEntryWidth])

	return recordPositionAtStore, nil
}

// nbrOfIndexes returns the nbr of index entries
func (idx *index) nbrOfIndexEntries() uint64 {
	return idx.size / indexEntryWidth
}

func (idx *index) close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return fmt.Errorf(indexContext+"faild to Sync/Flush back to device the mmap index file %s: %w", idx.name(), err)
	}

	if err := idx.file.Sync(); err != nil {
		return fmt.Errorf(indexContext+"failed to flush the index file %s to stable storage: %w", idx.name(), err)
	}

	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return fmt.Errorf(indexContext+"failed to truncate the index file %s to the last tracked size %d: %w", idx.name(), idx.size, err)
	}

	return idx.file.Close()
}

// Returns the Name of the index file
func (idx *index) name() string {
	return idx.file.Name()
}
