package comLog

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"

	"github.com/pkg/errors"
)

var encoding = binary.BigEndian

const (
	lenghtOfRecordSize = 8
	store_context      = "[store]: "
)

type store struct {
	mu       sync.RWMutex
	file     *os.File
	writeBuf *bufio.Writer
	size     uint64
	maxBytes uint64
}

func NewStore(file *os.File, maxBytes uint64) (*store, error) {
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, errors.Wrap(err, store_context+"Failed to init store")
	}
	// the size of the buffer is (defaultBufSize = 4096)
	// TODO: maybe make it with NewWriterSize to set the size of the buffer based on some conf
	var newWriteBuf *bufio.Writer = bufio.NewWriter(file)
	return &store{file: file, writeBuf: newWriteBuf, size: uint64(fileInfo.Size()), maxBytes: maxBytes}, nil
}

func (st *store) append(b_record []byte) (int, uint64, error) {
	//TODO: append the record with the timestamp
	var pos uint64 = st.size // new position of the new record
	// write the size []byte of the record
	if err := binary.Write(st.writeBuf, encoding, uint64(len(b_record))); err != nil {
		return 0, 0, errors.Wrap(err, store_context+"Failed to append size-bytes of the record")
	}
	nn, err := st.writeBuf.Write(b_record)
	if err != nil {
		return 0, 0, errors.Wrap(err, store_context+"Failed to append record-bytes")
	}
	nn += lenghtOfRecordSize
	st.size += uint64(nn)
	return nn, pos, nil
}

func (st *store) read(position uint64) (int, []byte, error) {
	// TODO: figure out how to differentiate the record and the timestamp
	var nbr_read_bytes int
	var fetch_position int64 = int64(position)
	// https://cs.opensource.google/go/go/+/refs/tags/go1.19.1:src/bufio/bufio.go;l=626;drc=54182ff54a687272dd7632c3a963e036ce03cb7c
	// When you flush the buffer if buf.n == 0 we return from the method -> nothing new to flush
	if err := st.writeBuf.Flush(); err != nil {
		return 0, nil, errors.Wrap(err, store_context+"Failed to Flush before reading")
	}
	// fetch the size of the record
	var record_size_byte []byte = make([]byte, lenghtOfRecordSize)
	nn, err := st.file.ReadAt(record_size_byte, fetch_position)
	if err != nil {
		return 0, nil, errors.Wrap(err, store_context+"Failed to read size-bytes of the record")
	}
	nbr_read_bytes += nn
	// fetch record
	var size uint64 = encoding.Uint64(record_size_byte)
	var b_record []byte = make([]byte, size)
	fetch_position += lenghtOfRecordSize // add 8 bytes to seek to the start of the record
	nn, err = st.file.ReadAt(b_record, fetch_position)
	if err != nil {
		return 0, nil, errors.Wrap(err, store_context+"Failed to read record-bytes")
	}
	nbr_read_bytes += nn
	return nbr_read_bytes, b_record, nil
}

func (st *store) ReadAt(b []byte, position uint64) (int, error) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	nn, err := st.file.ReadAt(b, int64(position))
	if err != nil {
		return 0, errors.Wrap(err, store_context+"Faile to read at")
	}
	return nn, nil
}

func (st *store) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := st.writeBuf.Flush(); err != nil {
		return errors.Wrap(err, store_context+"Faile to close the store file")
	}
	return st.file.Close()
}
