package client

import (
	"context"
	"io"
	"sync"
	"time"

	pb "github.com/ayoyu/comLog/api"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type (
	// Record represents the record data in bytes to send to the log server to be appended.
	Record = pb.Record

	// Offset represents the integer offset with which we can read a record from the log server.
	Offset = pb.Offset

	// BatchRecords represents a batch of `Record` to send.
	BatchRecord = pb.BatchRecords

	// AppendRecordResp represents the server response from appending a record.
	// The `offset` indicates the offset at which the record was assigned in the log
	// while the `nbrOfStoredBytes` indicates the number of bytes that were stored in the log.
	AppendResponse = pb.AppendRecordResp

	// SendAppendResponse represents the server response from appending a record from a batch
	// append operation.
	SendAppendResponse = pb.StreamAppendRecordResp

	// BatchAppendResp represents the server response from appending a batch of records in the same rpc call.
	BatchAppendResponse = pb.BatchAppendResp

	// ReadRecordResp represents the server response from reading a record.
	// The `record` holds the actual record in bytes while the `nbrOfReadBytes` indicates
	// the number of bytes that we were able to read.
	ReadResponse = pb.ReadRecordResp
)

// OnCompletionSendCallback is a callback function to be invoked when the asynchronous send operation is done.
// It is recommended for the user to call wait.Done() at the end of the function to be able to track the running goroutines.
type OnCompletionSendCallback func(resp *SendAppendResponse, wait *sync.WaitGroup)

type ComLogClient interface {
	// Append synchronously sends a record to the remote log server. This operation will block waiting
	// for the `AppendResponse` response from the server which specifies the offset to which the
	// record was assigned and the number of bytes stored
	Append(ctx context.Context, record *Record) (*AppendResponse, error)

	// Send asynchronously sends a record to the remote log server and invoke the provided callback when
	// the send has been acknowledged. This operation will not block and will return immediatly once the record
	// has been added to the buffer of records waiting to be sent, this allow sending many records without blocking
	// to wait for the server response.
	// The send operation from the pending buffer will be performed in batch FIFO append mode.
	//
	// The callback will generally be executed in a background I/O goroutine that is responsible for turning these
	// records into requests and transmitting them to the remote log server, for this reason the callback should be fast
	// enough to not delay the sending of messages to the server from other goroutines.
	Send(ctx context.Context, record *Record, callback OnCompletionSendCallback) error

	// BatchAppend synchronously sends a batch of records to the remote log server.
	// This operation will block waiting for the `BatchAppendResponse` response from the server which represents
	// a slice of `pb.BatchAppendResp_RespWithOrder` containing the offset the record was assigned to, the number of
	// bytes stored for each record and the original index of the record inside the batch to identify the record that
	// was succefully appended since the batch append operation on the server side is asynchronous.
	//
	// If an error occurs while appending records, the batch operation will stop and return the records that have been
	// successfully stored so far with their original indexes in the batch and the error that caused the shutdown, which
	// means in the worst case scenario where no record was successfully appended we can have a `BatchAppendResponse` with
	// an empty slice and the first encountered error that caused this.
	BatchAppend(ctx context.Context, batch *BatchRecord) (*BatchAppendResponse, error)

	// Read synchronously reads the record corresponding to the given offset. This operation will block
	// waiting for the `ReadResponse` response from the server which includes the recod in bytes `[]byte`
	// and the number of bytes we were able to read.
	Read(ctx context.Context, offset *Offset) (*ReadResponse, error)

	// Close shutdow the commit log client
	Close() error
}

type callbacks struct {
	mu    sync.RWMutex
	store map[int]OnCompletionSendCallback
}

func newCallbacks(size int) *callbacks {
	return &callbacks{store: make(map[int]OnCompletionSendCallback, size)}
}

func (c *callbacks) put(key int, val OnCompletionSendCallback) {
	c.mu.Lock()
	c.store[key] = val
	c.mu.Unlock()
}

func (c *callbacks) get(key int) (OnCompletionSendCallback, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.store[key]
	return val, ok
}

func (c *callbacks) setStore(newStore map[int]OnCompletionSendCallback) {
	c.mu.Lock()
	for k, v := range newStore {
		c.store[k] = v
	}
	c.mu.Unlock()
}

type comLogClient struct {
	remote   pb.ComLogRpcClient
	callOpts []grpc.CallOption
	batchOpt batchOption

	accumulator *recordAccumulator

	prevcallbacks *callbacks
	currcallbacks *callbacks

	wait *sync.WaitGroup

	closeCh         chan chan error
	streamRecvErrCh chan error

	lg *zap.Logger
}

func NewClientComLog(c *Client) ComLogClient {
	cli := &comLogClient{
		remote:          pb.NewComLogRpcClient(c.conn),
		callOpts:        c.callOpts,
		batchOpt:        c.batch,
		accumulator:     newRecordAccumulator(c.batch.batchSize),
		prevcallbacks:   newCallbacks(c.batch.batchSize),
		currcallbacks:   newCallbacks(c.batch.batchSize),
		wait:            new(sync.WaitGroup),
		closeCh:         make(chan chan error),
		streamRecvErrCh: make(chan error, 1),
		lg:              c.lg,
	}

	go cli.sendLoop()

	return cli
}

func (c *comLogClient) Append(ctx context.Context, record *Record) (*AppendResponse, error) {
	pb_out, err := c.remote.Append(ctx, record, c.callOpts...)
	if err != nil {
		return nil, err
	}

	return pb_out, nil
}

func (c *comLogClient) BatchAppend(ctx context.Context, batch *BatchRecord) (*BatchAppendResponse, error) {
	return c.remote.BatchAppend(ctx, batch, c.callOpts...)
}

func (c *comLogClient) Read(ctx context.Context, offset *Offset) (*ReadResponse, error) {
	pb_out, err := c.remote.Read(ctx, offset, c.callOpts...)
	if err != nil {
		return nil, err
	}

	return pb_out, nil
}

func (c *comLogClient) Close() error {
	errCh := make(chan error)
	c.closeCh <- errCh

	return <-errCh
}

func (c *comLogClient) sendBatch() error {
	batch := c.accumulator.prepareRecordBatch()
	if len(batch.Batch) == 0 {
		c.lg.Info("Nothing to send, the accumulator records batch is empty")
		return nil
	}

	stream, err := c.remote.StreamBatchAppend(context.TODO(), &batch, c.callOpts...)
	if err != nil {
		c.lg.Error("Failed to send the stream batch records", zap.Error(err))
		return err
	}
	// Once we set the prevcallbacks, the current callbacks can be overwritten when we
	// send the doneSending signal to the accumulator. In this case the accumulator can contiue
	// adding next record without blocking to wait for the whole stream receive operation
	c.prevcallbacks.setStore(c.currcallbacks.store)

	// The stream receive operation
	c.wait.Add(1)
	go func() {
		defer c.wait.Done()

		sem := make(chan struct{}, 100) // TODO(?)
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				c.streamRecvErrCh <- err
				break
			}

			callback, ok := c.prevcallbacks.get(int(resp.Index))

			if ok {
				sem <- struct{}{}
				c.wait.Add(1)
				go callback(resp, c.wait)
				<-sem
			}
		}
	}()

	return nil
}

func (c *comLogClient) sendLoop() {
	var (
		lastErr          error
		resetLingerTimer bool = true
		waitLinger       <-chan time.Time
	)

	for {
		if resetLingerTimer {
			waitLinger = time.NewTimer(c.batchOpt.linger).C
		}

		select {
		case err := <-c.streamRecvErrCh:
			lastErr = err
			resetLingerTimer = false

		case errCh := <-c.closeCh:
			// TODO: change the log msg
			c.lg.Info("Waiting for the current operations to finish")
			c.lg.Sync()

			c.wait.Wait()
			errCh <- lastErr
			return

		case <-c.accumulator.startSending():
			// Coming from an `append` event
			c.lg.Info("Accumulator record buffer is full. Start sending the batch records...")
			lastErr = c.sendBatch()
			c.accumulator.doneSending() <- struct{}{}
			resetLingerTimer = false

		case <-waitLinger:
			c.lg.Info("Wait linger time is triggered. Start sending the batch records...",
				zap.Duration("Wait linger time", c.batchOpt.linger))

			lastErr = c.sendBatch()
			c.accumulator.resetNextOffsetAndIndex()
			resetLingerTimer = true
		}
	}
}

func (c *comLogClient) Send(ctx context.Context, record *Record, callback OnCompletionSendCallback) error {
	IndexPos, err := c.accumulator.append(ctx, record.Data)

	if err == nil {
		c.currcallbacks.put(IndexPos, callback)
	}

	return err
}
