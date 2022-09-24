package comLog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var DefaultMaxBytes uint64 = 4096

const test_store_file = "test_store_file_"

func getStore(maxBytes uint64) (*store, error) {
	file := getTempfile(test_store_file)
	store, err := NewStore(file, maxBytes)
	return store, err
}

func TestNewStore(t *testing.T) {
	store, err := getStore(DefaultMaxBytes)
	assert.Equal(t, err, nil)
	fileInfo := getFileInfo(store.file)
	// Size will access len(writeBuf.buf) where buf is []byte
	assert.Equal(t, store.writeBuf.Size(), int(DefaultMaxBytes), "error: len(write.buf) != maxBytes")
	assert.Equal(t, store.size, uint64(fileInfo.Size()), "store.size != file.size")
	assert.Equal(t, store.maxBytes, DefaultMaxBytes)
}

type DataTestCases struct {
	casename string
	record   []byte
	nn       int
	pos      uint64
}

func getTestCases() []DataTestCases {
	key1, key2, key3 := "First Entry", "Second Entry", "Third Entry"
	pos1 := uint64(0)
	pos2 := uint64(len(key1)) + lenghtOfRecordSize        // from previous written key1
	pos3 := pos2 + uint64(len(key2)) + lenghtOfRecordSize // from previous written key1+key2
	testcases := []DataTestCases{
		{key1, []byte(key1), len(key1) + lenghtOfRecordSize, pos1},
		{key2, []byte(key2), len(key2) + lenghtOfRecordSize, pos2},
		{key3, []byte(key3), len(key3) + lenghtOfRecordSize, pos3},
	}
	return testcases
}

func TestAppend(t *testing.T) {
	testcases := getTestCases()
	store, _ := getStore(DefaultMaxBytes)
	curr_buffered_bytes_data := 0
	for _, case_s := range testcases {
		t.Logf(case_s.casename)
		nn, pos, err := store.append(case_s.record)
		assert.Equal(t, err, nil, "err is not nil")
		assert.Equal(t, nn, case_s.nn, "nn written bytes is not correct")
		assert.Equal(t, pos, case_s.pos, "curr pos of record is not correct")
		curr_buffered_bytes_data += case_s.nn
		assert.Equal(t, store.writeBuf.Buffered(), curr_buffered_bytes_data)
	}
}

func TestRead(t *testing.T) {
	testcases := getTestCases()
	store, _ := getStore(DefaultMaxBytes)
	for _, case_s := range testcases {
		t.Logf("Write: " + case_s.casename)
		_, pos, _ := store.append(case_s.record)
		t.Logf("Read: " + case_s.casename)
		nn, read_record, err := store.read(pos)
		assert.Equal(t, err, nil, "err is not nil")
		assert.Equal(t, nn, case_s.nn, "read nn bytes is not correct")
		assert.Equal(t, read_record, case_s.record, "record written != readed record")

	}
}

func TestClose(t *testing.T) {
	file := getTempfile(test_store_file)
	fileInfo := getFileInfo(file)
	assert.Equal(t, int(fileInfo.Size()), 0)
	store, _ := NewStore(file, DefaultMaxBytes)
	// make some writes to test the buffer flush
	testcases := getTestCases()
	curr_buffered_bytes_data := 0
	for _, case_s := range testcases {
		store.append(case_s.record)
		curr_buffered_bytes_data += case_s.nn
	}
	err := store.close()
	assert.Equal(t, err, nil)
	reopenFile, err := reopenClosedFile(store.Name())
	assert.Equal(t, err, nil)
	reopenFileInfo := getFileInfo(reopenFile)
	assert.Equal(
		t, int(reopenFileInfo.Size()), curr_buffered_bytes_data,
		"Closed/flushed file store size != buffered data size",
	)

}

func TestStoreName(t *testing.T) {
	store, _ := getStore(DefaultMaxBytes)
	assert.Equal(t, store.Name(), store.file.Name())
}
