package comLog

import (
	"errors"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	TEST_LOG_DIR = "test_log_dir_"
	MAX_BYTES    = 4096
)

func createLogDir(baseoffsets []string) (string, error) {
	dir, err := getTempDir(TEST_LOG_DIR)
	if err != nil {
		return "", err
	}

	for _, base := range baseoffsets {
		storepath := path.Join(dir, base+".store")
		indexpath := path.Join(dir, base+".index")
		_, err = os.OpenFile(storepath, os.O_CREATE, 0666)
		if err != nil {
			return "", err
		}
		_, err = os.OpenFile(indexpath, os.O_CREATE, 0666)
		if err != nil {
			return "", err
		}
	}
	return dir, nil
}

func createConfig(dir string, storeMaxBytes, idxMaxBytes uint64) Config {
	if storeMaxBytes == 0 {
		storeMaxBytes = MAX_BYTES
	}
	if idxMaxBytes == 0 {
		idxMaxBytes = MAX_BYTES
	}
	return Config{
		Data_dir:      dir,
		StoreMaxBytes: storeMaxBytes,
		IndexMaxBytes: idxMaxBytes,
	}
}

func TestLogSetupFomPreviousRun(t *testing.T) {
	baseoffsets := []string{"40", "10", "0", "20"}
	log_dir, err := createLogDir(baseoffsets)
	assert.Nil(t, err)
	// defer removeTempDir(log_dir)
	conf := createConfig(log_dir, 0, 0)
	log := &Log{Config: conf}
	err = log.setup()
	assert.Nil(t, err)
	defer func() {
		t.Logf("Deleting the log from %s ...", log_dir)
		err = log.Remove()
		assert.Nil(t, err, "cannot remove the log from %s. Don't forget to remove it latter", log_dir)
	}()

	assert.Equal(t, len(log.segments), len(baseoffsets), 4)

	sortedoffsets := []string{"0", "10", "20", "40"}
	for i := 0; i < len(log.segments); i++ {
		seg := log.segments[i]
		baseoffset := sortedoffsets[i]
		assert.Equal(
			t, seg.storeFile.file.Name(),
			path.Join(log_dir, baseoffset+".store"),
		)
		assert.Equal(
			t, seg.indexFile.file.Name(),
			path.Join(log_dir, baseoffset+".index"),
		)

	}
	assert.NotNil(t, log.loadActiveSeg())
	assert.Equal(
		t, log.loadActiveSeg().indexFile.file.Name(),
		path.Join(log_dir, "40.index"),
	)
	assert.Equal(
		t, log.loadActiveSeg().storeFile.file.Name(),
		path.Join(log_dir, "40.store"),
	)
}

func TestLogSetupFirstRun(t *testing.T) {
	log_dir, err := getTempDir(TEST_LOG_DIR)
	assert.Nil(t, err)

	conf := createConfig(log_dir, 0, 0)
	log := &Log{Config: conf}
	err = log.setup()
	assert.Nil(t, err)

	defer func() {
		t.Logf("Deleting the log from %s ...", log_dir)
		err = log.Remove()
		assert.Nil(t, err, "cannot remove the log from %s. Don't forget to remove it latter", log_dir)
	}()

	assert.Equal(t, len(log.segments), 1)
	assert.NotNil(t, log.loadActiveSeg())
	assert.Equal(
		t, log.loadActiveSeg().indexFile.file.Name(),
		path.Join(log_dir, "0.index"),
	)
	assert.Equal(
		t, log.loadActiveSeg().storeFile.file.Name(),
		path.Join(log_dir, "0.store"),
	)
}

func TestLogSegmentSearch(t *testing.T) {
	conf := Config{}
	log := &Log{Config: conf}
	log.segments = []*Segment{
		{baseOffset: 0, nextOffset: 10},
		{baseOffset: 10, nextOffset: 20},
		{baseOffset: 20, nextOffset: 30},
		{baseOffset: 30, nextOffset: 40},
	}
	cases := map[int64]*Segment{
		0:  log.segments[0],
		10: log.segments[1],
		12: log.segments[1],
		6:  log.segments[0],
		20: log.segments[2],
		31: log.segments[3],
		-1: log.segments[3], // last seg (valid offset)
		40: nil,
		55: nil,
	}
	for offset, targetSeg := range cases {
		ans := log.segmentSearch(offset)
		assert.Equal(t, ans, targetSeg)
	}
}

func TestClosingLog(t *testing.T) {
	log_dir, err := getTempDir(TEST_LOG_DIR)
	assert.Nil(t, err)
	defer removeTempDir(log_dir)

	conf := createConfig(log_dir, 0, 0)
	log, err := NewLog(conf)
	assert.Nil(t, err)

	offset, _, err := log.Append([]byte("Hello"))
	assert.Nil(t, err)
	assert.Nil(t, log.Close())

	t.Logf("Log segments size: %d", log.SegmentsSize())

	_, _, err = log.Append([]byte("Foo"))
	t.Logf("Closing Error: %s", err)
	assert.True(t, errors.Is(err, os.ErrClosed))

	_, _, err = log.Read(int64(offset))
	t.Logf("Closing Error: %s", err)
	assert.True(t, errors.Is(err, os.ErrClosed))
}

func TestSuccessfulCollectSegments(t *testing.T) {
	log_dir, err := getTempDir(TEST_LOG_DIR)
	assert.Nil(t, err)
	defer removeTempDir(log_dir)

	const (
		segMaxBytes    = 100
		recordSize     = 50
		iterations     = 1000
		LenRecordWidth = 8
	)
	conf := createConfig(log_dir, segMaxBytes, segMaxBytes)
	log, err := NewLog(conf)
	assert.Nil(t, err)

	// this will lead into 2 records per segment i.e. 2 records in the store 50*2=100 bytes and 8*2=16 bytes in the index.
	// The store will get filled up and will trigger the creation of the next segment
	rec := generateRandomRecord(recordSize)
	totalBytes := 0
	totalStoreBytes := 0
	for i := 0; i < iterations; i++ {
		_, nn, err := log.Append(rec)
		totalBytes += nn
		totalStoreBytes += (nn - LenRecordWidth)
		assert.Nil(t, err)
	}

	currSegsSize := log.SegmentsSize()
	assert.Equal(t, totalBytes, (recordSize+8)*iterations)
	assert.Equal(t, currSegsSize, (totalStoreBytes / segMaxBytes), "Nbr of segments should be equal 500")
	t.Logf(
		"Number of segments: %d, Total Bytes: %d, Total Store Bytes: %d, Nbr Segments: %d",
		currSegsSize, totalBytes, totalStoreBytes, (totalStoreBytes / segMaxBytes))

	assert.Equal(t, log.OldestOffset(), uint64(0))
	// Collect half the segments
	// Offsets should be in the range 0 to 999
	// Offset=500 should indicates that we want to get rid off the old half
	var offset uint64 = 500
	err = log.CollectSegments(offset)
	assert.Nil(t, err)
	assert.Equal(t, log.SegmentsSize(), currSegsSize/2, "The new nbr of segments should be equal 250")
	assert.NotEqual(t, log.OldestOffset(), uint64(0), "The oldest offset after collecting half the segments must be different than 0")

	// Collect Nothing
	err = log.CollectSegments(0)
	assert.Nil(t, err)
	assert.Equal(t, log.SegmentsSize(), currSegsSize/2, "The new nbr of segments should be equal 250")
	assert.NotEqual(t, log.OldestOffset(), uint64(0), "The oldest offset after collecting half the segments must be different than 0")

	// Collect ALL
	// The log should be reconfigured because we will delete all the segments
	lastOffset := log.LastOffset()
	t.Logf("The last tracked offset before collecting all the segments: %d", lastOffset)
	err = log.CollectSegments(lastOffset)
	assert.Nil(t, err)
	assert.Equal(t, log.SegmentsSize(), 1, "The new nbr of segments should be equal 1")
	assert.Equal(t, log.LastOffset(), uint64(0), "The last offset should be equal 0")
	assert.Equal(t, log.OldestOffset(), uint64(0), "The oldset offset should be equal 0")

	// Append after collecting all segments
	helloOffset, _, err := log.Append([]byte("Hello"))
	assert.Nil(t, err)
	assert.Equal(t, helloOffset, uint64(0))
	assert.Equal(t, log.LastOffset(), helloOffset+1, "The last offset should be equal 1")
	assert.Equal(t, log.OldestOffset(), uint64(0), "The oldset offset should be equal 0")
}

func TestInvalidOffsetArgumentErr(t *testing.T) {
	log_dir, err := getTempDir(TEST_LOG_DIR)
	assert.Nil(t, err)
	defer removeTempDir(log_dir)

	conf := createConfig(log_dir, 0, 0)
	log, err := NewLog(conf)
	assert.Nil(t, err)

	_, _, err = log.Read(-2)
	t.Logf("Invalid Offset Argument: %s", err)
	assert.True(t, errors.Is(err, InvalidOffsetArgError))
}

func TestOutOfRangeErr(t *testing.T) {
	log_dir, err := getTempDir(TEST_LOG_DIR)
	assert.Nil(t, err)
	defer removeTempDir(log_dir)

	conf := createConfig(log_dir, 0, 0)
	log, err := NewLog(conf)
	assert.Nil(t, err)

	_, _, err = log.Read(10)
	t.Logf("Out Of Range Offset: %s", err)
	assert.True(t, errors.Is(err, OutOfRangeError))
}
