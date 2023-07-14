package comLog

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

const test_segment_dir = "test_segment_dir_"

func TestNewSegment_EmptyIndexFile(t *testing.T) {
	// from an empty index file
	dirpath, err := getTempDir(test_segment_dir)
	assert.Nil(t, err)
	// remove temp test data
	defer removeTempDir(dirpath)

	var baseOffset uint64 = 0
	segment, err := NewSegment(dirpath, DefaultMaxBytesStore, DefaultMaxBytesIndex, baseOffset)
	assert.Nil(t, err)
	assert.IsType(t, &store{}, segment.storeFile)
	assert.IsType(t, &index{}, segment.indexFile)
	assert.Equal(t, baseOffset, segment.baseOffset)
	assert.Equal(t, baseOffset, segment.nextOffset)
	assert.Equal(t, dirpath, segment.path)
	t.Logf("dir: %s, storefile: %s, indexfile: %s", dirpath, segment.storeFile.name(), segment.indexFile.name())
}

func TestNewSegment_NotEmptyIndexFile(t *testing.T) {
	// from not an empty index file
	var baseOffset uint64 = 100
	dirpath, err := getTempDir(test_segment_dir)
	assert.Nil(t, err)

	tempfilepath := path.Join(dirpath, "100.index")
	tempfile, err := os.OpenFile(tempfilepath, os.O_CREATE, 0666)
	assert.Nil(t, err)
	// simulate some writes of 2 entries -> 2 * 16 byte
	var size int64 = 2 * indexWidth
	err = os.Truncate(tempfile.Name(), size)
	assert.Nil(t, err)

	segment, err := NewSegment(dirpath, DefaultMaxBytesStore, DefaultMaxBytesIndex, baseOffset)
	assert.Equal(t, segment.nextOffset, baseOffset+2)
	assert.Equal(t, int(segment.indexFile.nbrOfIndexes()), 2)
	removeTempDir(dirpath)
}

func TestSegmentIsFull(t *testing.T) {
	dirpath, err := getTempDir(test_segment_dir)
	assert.Nil(t, err)
	defer removeTempDir(dirpath)

	var baseOffset uint64 = 0
	var smaxBytes uint64 = 0 // trigger the Full
	var idxmaxBytes uint64 = 10
	segment, err := NewSegment(dirpath, smaxBytes, idxmaxBytes, baseOffset)
	assert.Nil(t, err)
	assert.Equal(t, segment.isFull(), true)

}

func TestGet_indexPath_storagePath(t *testing.T) {
	dirpath, err := getTempDir(test_segment_dir)
	assert.Nil(t, err)
	defer removeTempDir(dirpath)

	var baseOffset uint64 = 0
	seg, err := NewSegment(dirpath, DefaultMaxBytesStore, DefaultMaxBytesIndex, baseOffset)
	assert.Nil(t, err)
	assert.Equal(
		t,
		seg.getIndexPath(),
		path.Join(dirpath, "0.index"),
	)
	assert.Equal(
		t,
		seg.getStorePath(),
		path.Join(dirpath, "0.store"),
	)
}

type SegmentTestData struct {
	record []byte
	offset uint64
}

func TestSegmentBasicAppend(t *testing.T) {
	dirpath, err := getTempDir(test_segment_dir)
	assert.Nil(t, err)
	defer removeTempDir(dirpath)

	var baseOffset uint64 = 0
	seg, err := NewSegment(dirpath, DefaultMaxBytesStore, DefaultMaxBytesStore, baseOffset)
	assert.Nil(t, err)
	seg.setIsActive(true)

	testcases := []SegmentTestData{
		{[]byte("Hello Word"), baseOffset},
		{[]byte("Second Hello"), baseOffset + 1},
		{[]byte("Third Hello"), baseOffset + 2},
	}
	for _, case_s := range testcases {
		offset, nn, err := seg.Append(case_s.record)
		assert.Nil(t, err)
		assert.Equal(t, offset, case_s.offset)
		assert.Equal(t, seg.nextOffset, case_s.offset+1)
		assert.Equal(t, nn, len(case_s.record)+lenghtOfRecordSize)
	}

}

func TestSegmentBasicRead(t *testing.T) {
	dirpath, err := getTempDir(test_segment_dir)
	assert.Nil(t, err)
	defer removeTempDir(dirpath)

	var baseOffset uint64 = 100
	seg, err := NewSegment(dirpath, DefaultMaxBytesStore, DefaultMaxBytesStore, baseOffset)
	assert.Nil(t, err)
	seg.setIsActive(true)

	testcases := []SegmentTestData{
		{[]byte("Hello Word"), baseOffset},       // 100
		{[]byte("Second Hello"), baseOffset + 1}, // 101
		{[]byte("Third Hello"), baseOffset + 2},  // 102
	}
	for _, case_s := range testcases {
		// Write
		offset, _, err := seg.Append(case_s.record)
		assert.Nil(t, err)
		assert.Equal(t, offset, case_s.offset)
		t.Logf("Write: %s, offset: %d", case_s.record, offset)
		// Read back
		_, read_back_record, err := seg.Read(int64(offset))
		assert.Nil(t, err)
		t.Logf("Read Back: %s", string(read_back_record))
		assert.Equal(t, read_back_record, case_s.record)
		_, lastEntry, err := seg.Read(-1)
		t.Logf("Read Curr LastEntry: %s", lastEntry)
		assert.Equal(t, lastEntry, case_s.record)
		t.Logf("********************************")

	}
}
