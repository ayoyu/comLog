package comLog

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

const test_log_dir = "test_log_dir_"

func createLogDir(baseoffsets []string) (string, error) {
	dir := getTempDir(test_log_dir)
	var err error
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

func createConfig(dir string) Config {
	return Config{Data_dir: dir, StoreMaxBytes: 4096, IndexMaxBytes: 4096}
}

func TestLogSetupFomPreviousRun(t *testing.T) {
	baseoffsets := []string{"40", "10", "0", "20"}
	log_dir, err := createLogDir(baseoffsets)
	assert.Nil(t, err)
	defer removeTempDir(log_dir)
	conf := createConfig(log_dir)
	log := &Log{Config: conf}
	err = log.setup()
	assert.Nil(t, err)
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
	log_dir := getTempDir(test_log_dir)
	defer removeTempDir(log_dir)
	conf := createConfig(log_dir)
	log := &Log{Config: conf}
	err := log.setup()
	assert.Nil(t, err)
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
		&Segment{baseOffset: 0, nextOffset: 10},
		&Segment{baseOffset: 10, nextOffset: 20},
		&Segment{baseOffset: 20, nextOffset: 30},
		&Segment{baseOffset: 30, nextOffset: 40},
	}
	cases := map[int64]*Segment{
		0:  log.segments[0],
		10: log.segments[1],
		12: log.segments[1],
		6:  log.segments[0],
		20: log.segments[2],
		31: log.segments[3],
		-1: log.segments[3], // last seg
		40: nil,
		55: nil,
	}
	for offset, targetSeg := range cases {
		ans := log.segmentSearch(offset)
		assert.Equal(t, ans, targetSeg)
	}
}
