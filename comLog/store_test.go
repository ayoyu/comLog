package comLog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var DefaultMaxBytesStore uint64 = 4096

const test_store_file = "test_store_file_"

func getStore(maxBytes uint64) (*store, error) {
	file, err := getTempfile("", test_store_file)
	if err != nil {
		return nil, err
	}
	return newStore(file, maxBytes)
}

func TestNewStore(t *testing.T) {
	store, err := getStore(DefaultMaxBytesStore)
	assert.Nil(t, err)

	fileInfo, err := getFileInfo(store.file)
	assert.Nil(t, err)

	// Size will access len(writeBuf.buf) where buf is []byte
	assert.Equal(t, store.writeBuf.Size(), int(DefaultMaxBytesStore), "error: len(write.buf) != maxBytes")
	assert.Equal(t, store.size, uint64(fileInfo.Size()), "store.size != file.size")
	assert.Equal(t, store.maxBytes, DefaultMaxBytesStore)
	// remove temp test data
	removeTempFile(store.file.Name())
}

type StoreDataTestCases struct {
	casename string
	record   []byte
	nn       int
	pos      uint64
}

func getStoreTestCases() []StoreDataTestCases {
	key1, key2, key3 := "First Entry", "Second Entry", "Third Entry"
	pos1 := uint64(0)
	pos2 := uint64(len(key1)) + lenghtOfRecordSize        // from previous written key1
	pos3 := pos2 + uint64(len(key2)) + lenghtOfRecordSize // from previous written key1+key2
	testcases := []StoreDataTestCases{
		{key1, []byte(key1), len(key1) + lenghtOfRecordSize, pos1},
		{key2, []byte(key2), len(key2) + lenghtOfRecordSize, pos2},
		{key3, []byte(key3), len(key3) + lenghtOfRecordSize, pos3},
	}
	return testcases
}

func TestStoreAppend(t *testing.T) {
	testcases := getStoreTestCases()
	store, err := getStore(DefaultMaxBytesStore)
	assert.Nil(t, err)
	curr_buffered_bytes_data := 0
	for _, case_s := range testcases {
		t.Logf(case_s.casename)
		nn, pos, err := store.append(case_s.record)
		assert.Nil(t, err, "err is not nil")
		assert.Equal(t, nn, case_s.nn, "nn written bytes is not correct")
		assert.Equal(t, pos, case_s.pos, "curr pos of record is not correct")
		curr_buffered_bytes_data += case_s.nn
		assert.Equal(t, store.writeBuf.Buffered(), curr_buffered_bytes_data)
	}
	// remove temp test data
	removeTempFile(store.file.Name())
}

func TestStoreRead(t *testing.T) {
	testcases := getStoreTestCases()
	store, err := getStore(DefaultMaxBytesStore)
	assert.Nil(t, err)
	for _, case_s := range testcases {
		t.Logf("Write: " + case_s.casename)
		_, pos, err := store.append(case_s.record)
		assert.Nil(t, err)
		t.Logf("Read: " + case_s.casename)
		nn, read_record, err := store.read(pos)
		assert.Nil(t, err, "err is not nil")
		assert.Equal(t, nn, case_s.nn, "read nn bytes is not correct")
		assert.Equal(t, read_record, case_s.record, "record written != readed record")

	}
	// remove temp test data
	removeTempFile(store.file.Name())
}

func TestStoreClose(t *testing.T) {
	file, err := getTempfile("", test_store_file)
	assert.Nil(t, err)

	fileInfo, err := getFileInfo(file)
	assert.Nil(t, err)
	assert.Equal(t, int(fileInfo.Size()), 0)

	store, err := newStore(file, DefaultMaxBytesStore)
	assert.Nil(t, err)
	// make some writes to test the buffer flush
	testcases := getStoreTestCases()
	curr_buffered_bytes_data := 0
	for _, case_s := range testcases {
		store.append(case_s.record)
		curr_buffered_bytes_data += case_s.nn
	}
	err = store.close()
	assert.Nil(t, err)

	reopenFile, err := openFile(store.name())
	assert.Nil(t, err)
	reopenFileInfo, err := getFileInfo(reopenFile)
	assert.Nil(t, err)
	assert.Equal(
		t, int(reopenFileInfo.Size()), curr_buffered_bytes_data,
		"Closed/flushed file store size != buffered data size",
	)
	// remove temp test data
	removeTempFile(store.file.Name())

}

func TestStoreName(t *testing.T) {
	store, err := getStore(DefaultMaxBytesStore)
	assert.Nil(t, err)
	assert.Equal(t, store.name(), store.file.Name())
	// remove temp test data
	removeTempFile(store.file.Name())
}
