package comLog

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var encoding = binary.BigEndian

const (
	lenghtOfRecordSize = 8
	storeContext       = "[store]: "
)

type store struct {
	file     *os.File
	size     uint64
	maxBytes uint64
	mu       sync.Mutex
	writeBuf *bufio.Writer
}

func newStore(file *os.File, maxBytes uint64) (*store, error) {
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, fmt.Errorf(storeContext+"Failed to init store. Err: %w", err)
	}
	// the size of the buffer is (defaultBufSize = 4096)
	var newWriteBuf *bufio.Writer = bufio.NewWriterSize(file, int(maxBytes))

	return &store{
		file:     file,
		writeBuf: newWriteBuf,
		size:     uint64(fileInfo.Size()),
		maxBytes: maxBytes,
	}, nil
}

func (st *store) append(record []byte) (nn int, pos uint64, err error) {
	//TODO(new feat ??): append the record with the timestamp
	pos = st.size
	// Write first the length of the record. The record will be **prefixed** by its length
	if err = binary.Write(st.writeBuf, encoding, uint64(len(record))); err != nil {
		return 0, 0, fmt.Errorf(storeContext+"Failed to append size-bytes of the record. Err: %w", err)
	}

	nn, err = st.writeBuf.Write(record)
	if err != nil {
		return 0, 0, fmt.Errorf(storeContext+"Failed to append record-bytes. Err: %w", err)
	}

	// add the record length to the nbr of bytes written
	nn += lenghtOfRecordSize
	st.size += uint64(nn)

	return nn, pos, nil
}

func (st *store) read(position uint64) (int, []byte, error) {
	// https://cs.opensource.google/go/go/+/refs/tags/go1.19.1:src/bufio/bufio.go;l=626;drc=54182ff54a687272dd7632c3a963e036ce03cb7c
	// When you flush the buffer if buf.n == 0 we return from the method -> nothing new to flush
	// Implicit flushing for the store buffer
	st.mu.Lock()
	if err := st.writeBuf.Flush(); err != nil {
		st.mu.Unlock()
		return 0, nil, fmt.Errorf(storeContext+"Failed to Flush before reading. Err: %w", err)
	}
	st.mu.Unlock()

	var nbrBytes int
	var fetchPos int64 = int64(position)
	// Fetch first the length of the record to initialize the record buffer
	recordLen := make([]byte, lenghtOfRecordSize)
	nn, err := st.file.ReadAt(recordLen, fetchPos)
	if err != nil {
		return 0, nil, fmt.Errorf(storeContext+"Failed to read length bytes of the record at position %d. Err: %w", fetchPos, err)
	}

	nbrBytes += nn

	// Fetch the record
	var size uint64 = encoding.Uint64(recordLen)
	record := make([]byte, size)
	// add the lenghtOfRecordSize=8 bytes to seek to the start of the record
	fetchPos += lenghtOfRecordSize
	nn, err = st.file.ReadAt(record, fetchPos)
	if err != nil {
		return 0, nil, fmt.Errorf(storeContext+"Failed to read record bytes at position %d. Err: %w", fetchPos, err)
	}

	nbrBytes += nn

	return nbrBytes, record, nil
}

func (st *store) readAt(buf []byte, position uint64) (int, error) {
	nn, err := st.file.ReadAt(buf, int64(position))
	if err != nil {
		return 0, fmt.Errorf(storeContext+"Failed to record read at position %d. Err: %w", position, err)
	}

	return nn, nil
}

func (st *store) close() error {
	if err := st.writeBuf.Flush(); err != nil {
		return fmt.Errorf(storeContext+"Failed to close the store file. Err: %w", err)
	}

	return st.file.Close()
}

// Returns the Name of the store file
func (st *store) Name() string {
	return st.file.Name()
}
