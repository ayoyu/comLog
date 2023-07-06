package comLog

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offsetWidth   = 8
	positionWidth = 8
	indexWidth    = offsetWidth + positionWidth
	indexContext  = "[index]: "
)

var IndexOutOfRangeError = errors.New("the given offset is not yet filled (out of range)")

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
		return nil, fmt.Errorf(indexContext+"Failed to get fileInfo. Err: %w", err)
	}
	// Real size before growing the file index
	realFileSize := uint64(fileInfo.Size())
	// grow the size of the file with spaces to maxByte to get a mmap-buf with the same size
	err = os.Truncate(file.Name(), int64(maxBytes))
	if err != nil {
		return nil, fmt.Errorf(indexContext+"Failed to truncate index file to grow its size to maxBytes. Err: %w", err)
	}

	mmap, err = gommap.Map(file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf(indexContext+"Failed to mmap the index file. Err: %w", err)
	}

	return &index{
		file:     file,
		size:     realFileSize,
		mmap:     mmap,
		maxBytes: maxBytes,
	}, nil
}

func (idx *index) append(offset, position uint64) error {
	var currPos uint64 = idx.size
	// maxBytes = len(mmap)
	if idx.maxBytes-currPos < indexWidth {
		return fmt.Errorf(indexContext+"Failed to append (offset, position), no more space EOF. Err: %w", io.EOF)
	}

	encoding.PutUint64(idx.mmap[currPos:currPos+offsetWidth], offset)
	encoding.PutUint64(idx.mmap[currPos+offsetWidth:currPos+indexWidth], position)
	idx.size += indexWidth

	return nil
}

func (idx *index) read(offset int64) (uint64, error) {
	var pos uint64
	if offset == -1 {
		// last entry
		pos = idx.size - indexWidth
	} else {
		// offset must be scaled to the baseOffset
		pos = uint64(offset) * indexWidth
	}

	if pos+indexWidth > idx.size {
		// this pos is not yet filled
		return 0, IndexOutOfRangeError
	}
	var recordPos uint64 = encoding.Uint64(idx.mmap[pos+offsetWidth : pos+indexWidth])

	return recordPos, nil
}

// Returns nbr of index entries
func (idx *index) nbrOfIndexes() uint64 {
	return idx.size / indexWidth
}

func (idx *index) close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return fmt.Errorf(indexContext+"Faild to Sync/Flush back to device the mmap index file. Err: %w", err)
	}

	if err := idx.file.Sync(); err != nil {
		return fmt.Errorf(indexContext+"Failed to flush the index file to stable storage. Err: %w", err)
	}

	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return fmt.Errorf(indexContext+"Failed to truncate the index file to the last tracked size. Err: %w", err)
	}

	return idx.file.Close()
}

// Returns the Name of the index file
func (idx *index) Name() string {
	return idx.file.Name()
}
