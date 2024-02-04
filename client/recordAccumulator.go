package client

import (
	"context"
	"sync"
	"sync/atomic"

	pb "github.com/ayoyu/comLog/api"
)

type BufferOffset struct {
	start, end int
}

// RecordAccumulator acts as a queue that accumulates records to be sent to the server.
// The accumulator uses a bounded amount of memory and `append` calls will block when that
// memory is exhausted, unless this behavior is explicitly disabled.
type recordAccumulator struct {
	size int

	mu         sync.RWMutex
	nextOffset atomic.Int64
	buf        []byte

	indexes   []BufferOffset
	nextIndex atomic.Int64

	startSend chan struct{}
	doneSend  chan struct{}
}

func newRecordAccumulator(size int) *recordAccumulator {
	return &recordAccumulator{
		size: size,
		buf:  make([]byte, size, size),
		// we don't know in advance how many records we will have. Rough estimate: 20 bytes per record
		indexes:   make([]BufferOffset, 0, size/20),
		startSend: make(chan struct{}, 1),
		// NB: This chan is not buffered, because we want to have explicityly a signal rdv (i.e. blocking)
		// to reset the 2 critical atomic positions for the buffer and the index.
		// - In the append case we need necessarly to block as we are adding the next record (the one that has no room).
		doneSend: make(chan struct{}),
	}
}

func (r *recordAccumulator) add(record []byte) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	start := int(r.nextOffset.Load())
	end := start + len(record)

	r.nextOffset.Store(int64(end))

	copy(r.buf[start:end], record)

	currNextIdx := int(r.nextIndex.Load())
	if currNextIdx < len(r.indexes) {
		r.indexes[currNextIdx] = BufferOffset{
			start: start,
			end:   end,
		}
		r.nextIndex.Add(1)

		return currNextIdx
	}

	r.indexes = append(
		r.indexes, BufferOffset{
			start: start,
			end:   end,
		})
	r.nextIndex.Store(int64(len(r.indexes)))

	return len(r.indexes) - 1
}

func (r *recordAccumulator) startSending() <-chan struct{} { return r.startSend }
func (r *recordAccumulator) doneSending() chan<- struct{}  { return r.doneSend }

// Add the record to the accumulator and returns its offset.
func (r *recordAccumulator) append(ctx context.Context, record []byte) (int, error) {
	// NB: The reason why we choose to `r.mu.Lock` in the `r.add` func and not her is because
	// first it is helping to have a fine grained control, but the most important one to recall
	// is because when there is no room for any record when calling `r.append` the operation will
	// block by sending the startSend signal to the `sendLoop` and waits for the the `doneSend` signal,
	// meanwhile in the `sendLoop` we are calling `r.prepareRecordBatch` in `sendBatch`. For this reason
	// if the `r.mu.Lock` was already taken in the `r.append` that is waiting for the done signal,
	// `r.prepareRecordBatch` will be not able to proceed i.e. Deadlock!!
	if int(r.nextOffset.Load())+len(record) <= r.size {
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
		// TODO(to study): Maybe we can receive the signal just after having enough room for the next records
		// and to not wait for the whole send operation.
		r.resetNextOffsetAndIndex()
	}

	return r.add(record), nil
}

func (r *recordAccumulator) resetNextOffsetAndIndex() {
	r.nextOffset.Store(0)
	r.nextIndex.Store(0)
}

func (r *recordAccumulator) prepareRecordBatch() pb.BatchRecords {
	// NOW the Bug is located in the r.index we must reset this slice when we send the batch the same thing
	// we do with the buffer.
	r.mu.RLock()
	defer r.mu.RUnlock()

	idxSize := int(r.nextIndex.Load())
	if idxSize == 0 {
		return pb.BatchRecords{}
	}
	// this records batch will hold records in the same order as they are in the indexes slice.
	// The same positions that are returned back in `comLogClient.Send` and used to index the callbacks.
	//
	// On the server side the same order will be preserved and return back in the stream response to use it
	// when getting the callback from the callbacks map.
	batch := make([]*pb.Record, 0, idxSize)

	for i := 0; i < idxSize; i++ {
		offset := r.indexes[i]

		rd := pb.Record{
			Data: r.buf[offset.start:offset.end],
		}
		batch = append(batch, &rd)
	}

	return pb.BatchRecords{
		Batch: batch,
	}
}
