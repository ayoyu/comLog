package comLog

import (
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/tysonmote/gommap"
)

const (
	offsetWidth   = 8
	positionWidth = 8
	indexWidth    = offsetWidth + positionWidth
)

var OutOfRangeError = errors.New("[index]: The given offset is not yet filled (out of range)")

type index struct {
	file     *os.File
	size     uint64
	mmap     gommap.MMap
	maxBytes int64
}

func NewIndex(file *os.File, maxBytes int64) (*index, error) {
	var (
		err      error
		mmap     gommap.MMap
		fileInfo os.FileInfo
	)
	fileInfo, err = os.Stat(file.Name())
	if err != nil {
		return nil, errors.Wrap(err, "[index]: Failed to get fileInfo")
	}
	// grow the size of the file to maxByte to get a mmap-buf with the same size
	err = os.Truncate(file.Name(), maxBytes)
	if err != nil {
		return nil, errors.Wrap(err, "[index]: Failed to truncate index file to grow its size to maxBytes")
	}
	mmap, err = gommap.Map(file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "[index]: Failed to mmap the index file")
	}
	return &index{file: file, size: uint64(fileInfo.Size()), mmap: mmap, maxBytes: maxBytes}, nil
}

func (idx *index) append(offset, position uint64) error {
	var curr_pos uint64 = idx.size
	// maxBytes = len(mmap)
	if idx.maxBytes-int64(curr_pos) < indexWidth {
		return errors.Wrap(io.EOF, "[index]: Failed to append (offset, position) no more space EOF")
	}
	encoding.PutUint64(idx.mmap[curr_pos:curr_pos+offsetWidth], offset)
	encoding.PutUint64(idx.mmap[curr_pos+offsetWidth:curr_pos+indexWidth], position)
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
		return 0, OutOfRangeError
	}
	var record_position uint64 = encoding.Uint64(idx.mmap[pos+offsetWidth : pos+indexWidth])
	return record_position, nil
}

func (idx *index) nbrOfIndexes() uint64 {
	return idx.size / indexWidth
}

func (idx *index) close() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return errors.Wrap(err, "[index]: Faild to Sync/Flush back to device the mmap index file")
	}
	if err := idx.file.Sync(); err != nil {
		return errors.Wrap(err, "[index]: Failed to flush the index file to stable storage")
	}
	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return errors.Wrap(err, "[index]: Failed to truncate the index file to the last tracked size")
	}
	return idx.file.Close()
}
