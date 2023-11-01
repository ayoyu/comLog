package client

import (
	"context"
	"sync"

	pb "github.com/ayoyu/comLog/api"
)

type offset struct {
	start, end int
}

// RecordAccumulator acts as a queue that accumulates records to be sent to the server.
// The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
// this behavior is explicitly disabled.
type recordAccumulator struct {
	size int

	mu         sync.RWMutex
	lastOffset int
	buf        []byte
	index      []offset

	startSend chan struct{}
	doneSend  chan struct{}
}

func newRecordAccumulator(size int) *recordAccumulator {
	return &recordAccumulator{
		size: size,
		buf:  make([]byte, size, size),
		// we don't know in advance how many records we will have. Rough estimate: 20 bytes per record
		index:     make([]offset, 0, size/20),
		startSend: make(chan struct{}),
		doneSend:  make(chan struct{}),
	}
}

func (r *recordAccumulator) add(record []byte) int {
	end := r.lastOffset + len(record)
	copy(r.buf[r.lastOffset:end], record)

	r.index = append(
		r.index, offset{
			start: r.lastOffset,
			end:   end,
		})

	r.lastOffset = end
	return len(r.index) - 1
}

func (r *recordAccumulator) startSending() <-chan struct{} { return r.startSend }
func (r *recordAccumulator) doneSending() chan<- struct{}  { return r.doneSend }

// Add the record to the accumulator and returns its offset.
func (r *recordAccumulator) append(ctx context.Context, record []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lastOffset+len(record) <= r.size {
		return r.add(record), nil
	}

	select {
	case r.startSend <- struct{}{}:
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.doneSend:
		// TODO(to study): Maybe we can receive the signal just after having enough room for the next record
		// to not wait for the whole send operation.
		r.lastOffset = 0
	}

	return r.add(record), nil
}

func (r *recordAccumulator) prepareRecordBatch() pb.BatchRecords {
	r.mu.RLock()
	defer r.mu.RUnlock()

	batch := make([]*pb.Record, 0, len(r.index))

	for _, offset := range r.index {
		r := pb.Record{
			Data: r.buf[offset.start:offset.end],
		}
		batch = append(batch, &r)
	}

	return pb.BatchRecords{
		Batch: batch,
	}
}
