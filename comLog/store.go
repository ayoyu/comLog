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
	recordLenWidthPrefix = 8
	storeContext         = "[store]: "
)

type store struct {
	file     *os.File
	size     uint64
	maxBytes uint64

	mu  sync.Mutex
	buf *bufio.Writer
}

func newStore(file *os.File, maxBytes uint64) (*store, error) {
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, fmt.Errorf(storeContext+"failed to get file stat for file %s: %w", file.Name(), err)
	}

	bufioWriter := bufio.NewWriterSize(file, int(maxBytes))

	return &store{
		file:     file,
		size:     uint64(fileInfo.Size()),
		maxBytes: maxBytes,
		buf:      bufioWriter,
	}, nil
}

// TODO(new feat ??): Append the record with its timestamp.
func (st *store) append(record []byte) (nn int, pos uint64, err error) {
	pos = st.size
	if err = binary.Write(st.buf, encoding, uint64(len(record))); err != nil {
		return 0, 0, fmt.Errorf(storeContext+"failed to append the prefix record length: %w", err)
	}

	nn, err = st.buf.Write(record)
	if err != nil {
		return 0, 0, fmt.Errorf(storeContext+"failed to append record: %w", err)
	}

	// We add the record length to the nbr of written bytes
	nn += recordLenWidthPrefix
	st.size += uint64(nn)

	return nn, pos, nil
}

func (st *store) read(position uint64) (int, []byte, error) {
	// When you flush the buffer if buf.n == 0 we return from the method, i.e. nothing new to flush
	// see https://cs.opensource.google/go/go/+/refs/tags/go1.19.1:src/bufio/bufio.go;l=626;drc=54182ff54a687272dd7632c3a963e036ce03cb7c
	st.mu.Lock()
	if err := st.buf.Flush(); err != nil {
		st.mu.Unlock()
		return 0, nil, fmt.Errorf(storeContext+"failed to flush the write buffer for file %s before reading: %w", st.name(), err)
	}
	st.mu.Unlock()

	var nbrBytes int
	fetchPosition := int64(position)

	recordLength := make([]byte, recordLenWidthPrefix)
	nn, err := st.file.ReadAt(recordLength, fetchPosition)
	if err != nil {
		return 0, nil, fmt.Errorf(storeContext+"failed to read length bytes of the record at position %d: %w", fetchPosition, err)
	}

	nbrBytes += nn

	record := make([]byte, encoding.Uint64(recordLength))
	fetchPosition += recordLenWidthPrefix
	nn, err = st.file.ReadAt(record, fetchPosition)
	if err != nil {
		return 0, nil, fmt.Errorf(storeContext+"failed to read record bytes at position %d: %w", fetchPosition, err)
	}

	nbrBytes += nn

	return nbrBytes, record, nil
}

func (st *store) readAt(buf []byte, position uint64) (int, error) {
	st.mu.Lock()
	if err := st.buf.Flush(); err != nil {
		st.mu.Unlock()
		return 0, fmt.Errorf(storeContext+"failed to flush the write buffer for file %s before `readAt`: %w", st.name(), err)
	}
	st.mu.Unlock()

	nn, err := st.file.ReadAt(buf, int64(position))
	if err != nil {
		return 0, fmt.Errorf(storeContext+"failed to read record at position %d: %w", position, err)
	}

	return nn, nil
}

func (st *store) close() error {
	if err := st.buf.Flush(); err != nil {
		return fmt.Errorf(storeContext+"failed to flush the write buffer for file %s before closing: %w", st.name(), err)
	}

	return st.file.Close()
}

// Returns the Name of the store file
func (st *store) name() string {
	return st.file.Name()
}
