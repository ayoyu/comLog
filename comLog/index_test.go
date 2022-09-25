package comLog

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

var DefaultMaxBytesIndex uint64 = 4096

const test_index_file = "test_index_file_"

func getIndex(maxbytes uint64) (*index, error) {
	file := getTempfile(test_index_file)
	index, err := newIndex(file, maxbytes)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func TestNewIndex(t *testing.T) {
	file := getTempfile(test_index_file)
	fileInfo := getFileInfo(file) // before Truncate the file (realSize)
	assert.NotEqual(t, fileInfo.Size(), DefaultMaxBytesIndex)
	index, err := newIndex(file, DefaultMaxBytesIndex)
	assert.Nil(t, err)
	assert.Equal(t, int64(index.size), fileInfo.Size())
	assert.Equal(t, len(index.mmap), int(DefaultMaxBytesIndex))
	assert.Equal(t, index.maxBytes, DefaultMaxBytesIndex)
	// remove the temp test data
	removeTempFile(file.Name())
}

type IndexDataTestCases struct {
	offset   uint64
	position uint64
}

func trackMmapBuffer(buf []byte) uint64 {
	var empty_slots uint64
	for i := len(buf) - 1; i >= 0; i-- {
		if buf[i] != 0 {
			break
		}
		empty_slots++
	}
	return uint64(len(buf)) - empty_slots
}

func TestIndexAppend(t *testing.T) {
	index, err := getIndex(DefaultMaxBytesIndex)
	assert.Nil(t, err)
	testcases := []IndexDataTestCases{
		{3, 4}, {10, 38}, {39, 10},
	} // in a real scenario offsets must be increased always by 1 to simulate
	// an array indexing
	var curr_buffered_byte_index uint64 = 0
	for _, case_s := range testcases {
		err = index.append(case_s.offset, case_s.position)
		t.Logf("append offset, pos: %d, %d", case_s.offset, case_s.position)
		assert.Nil(t, err)
		curr_buffered_byte_index += indexWidth
		assert.Equal(t, index.size, curr_buffered_byte_index)
		curr_buf_size := trackMmapBuffer(index.mmap)
		assert.Equal(t, curr_buf_size, curr_buffered_byte_index)
	}
	// remove the temp test data
	removeTempFile(index.file.Name())
}

func TestIndexAppendEOF(t *testing.T) {
	var _maxBytes uint64 = 5 // less then 16 bits = indexWidth
	index, err := getIndex(_maxBytes)
	assert.Nil(t, err)
	err = index.append(4, 2)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(
		t, err.Error(),
		"[index]: Failed to append (offset, position), no more space EOF: EOF",
	)
	// remove the temp test data
	removeTempFile(index.file.Name())
}

func TestIndexRead(t *testing.T) {
	index, err := getIndex(DefaultMaxBytesIndex)
	assert.Nil(t, err)
	testcases := []IndexDataTestCases{
		{0, 43}, {1, 60}, {2, 99},
	} // these are scaled offset (note: the scaling is done at the segment level)
	for _, case_s := range testcases {
		err = index.append(case_s.offset, case_s.position)
		assert.Nil(t, err)
		read_pos, err := index.read(int64(case_s.offset))
		assert.Nil(t, err)
		last_entry_pod, err := index.read(-1)
		assert.Nil(t, err)
		assert.Equal(t, read_pos, case_s.position)
		assert.Equal(t, last_entry_pod, case_s.position)
	}
	// remove the temp test data
	removeTempFile(index.file.Name())
}

func TestIndexReadOutofRangeError(t *testing.T) {
	index, err := getIndex(DefaultMaxBytesIndex)
	assert.Nil(t, err)
	pos, err := index.read(1)
	assert.Equal(t, int(pos), 0)
	assert.ErrorIs(t, err, OutOfRangeError)
	assert.Equal(
		t, err.Error(),
		"[index]: The given offset is not yet filled (out of range)",
	)
	// remove the temp test data
	removeTempFile(index.file.Name())
}

func TestIndexNbrOfIndexes(t *testing.T) {
	index, err := getIndex(DefaultMaxBytesIndex)
	assert.Nil(t, err)
	testcases := []IndexDataTestCases{
		{0, 43}, {1, 60}, {2, 99},
	}
	var count uint64 = 0
	for _, case_s := range testcases {
		index.append(case_s.offset, case_s.position)
		count++
		assert.Equal(t, index.nbrOfIndexes(), count)
	}
	// remove the temp test data
	removeTempFile(index.file.Name())
}

func TestIndexClose(t *testing.T) {
	index, err := getIndex(DefaultMaxBytesIndex)
	assert.Nil(t, err)
	fileInfo := getFileInfo(index.file) // after(NewIndex) Truncate the file
	assert.Equal(t, fileInfo.Size(), int64(DefaultMaxBytesIndex))
	// write some entries to test the Sync/Flush/Truncate
	testcases := []IndexDataTestCases{
		{0, 43}, {1, 60}, {2, 99},
	}
	for _, case_s := range testcases {
		index.append(case_s.offset, case_s.position)
	}
	lastSize := int64(index.size)
	err = index.close() // will truncate the file to real size = lastSize
	assert.Nil(t, err)

	reopenFile, err := reopenClosedFile(index.Name())
	assert.Nil(t, err)
	reopenFileInfo := getFileInfo(reopenFile)
	assert.Equal(t, reopenFileInfo.Size(), lastSize)
	// remove the temp test data
	removeTempFile(index.file.Name())
}
