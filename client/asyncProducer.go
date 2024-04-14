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

	// SendAppendResponse represents the server response from appending a record from a batch
	// append operation.
	SendAppendResponse = pb.StreamAppendRecordResp
)

// OnCompletionSendCallback is a callback function to be invoked when the asynchronous send operation is done.
// It is recommended for the user to call wait.Done() at the end of the function to be able to track the running goroutines.
type OnCompletionSendCallback func(resp *SendAppendResponse, wait *sync.WaitGroup)

type AsyncProducer interface {
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

	// Errors is the error output channel back to the user. If `client.WithAsyncProducerReturnErrors` is set to true,
	// You MUST read from this channel or the Producer will **Deadlock**.
	// Otherwise if set to false (by default), this will prevents errors to be communicated back to the user.
	Errors() <-chan error

	// Close shuts down the async producer and sends any remaining buffered records from the record accumulator
	// while waiting for the current operations to finish.
	// It must be called on the producer side in order to avoid leaks and message lost.
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

type asyncProducer struct {
	remote   pb.ComLogRpcClient
	callOpts []grpc.CallOption
	batchOpt batchOption

	accumulator *recordAccumulator

	prevcallbacks *callbacks
	currcallbacks *callbacks

	wait *sync.WaitGroup

	closeCh         chan chan error
	streamRecvErrCh chan error

	enableReturnProducerErr bool
	producerErr             chan error

	lg *zap.Logger
}

func NewAsyncProducer(c *Client) AsyncProducer {
	p := &asyncProducer{
		remote:                  pb.NewComLogRpcClient(c.conn),
		callOpts:                c.callOpts,
		batchOpt:                c.batch,
		accumulator:             newRecordAccumulator(c.batch.batchSize),
		prevcallbacks:           newCallbacks(c.batch.batchSize),
		currcallbacks:           newCallbacks(c.batch.batchSize),
		wait:                    new(sync.WaitGroup),
		closeCh:                 make(chan chan error),
		streamRecvErrCh:         make(chan error, 1),
		enableReturnProducerErr: c.pReturnErr.enabled,
		producerErr:             make(chan error),
		lg:                      c.lg,
	}

	go p.sendLoop()

	return p
}

func (p *asyncProducer) Errors() <-chan error {
	return p.producerErr
}

func (p *asyncProducer) Close() error {
	errCh := make(chan error)
	p.closeCh <- errCh

	return <-errCh
}

func (p *asyncProducer) sendBatch() error {
	batch := p.accumulator.prepareRecordBatch()
	if len(batch.Batch) == 0 {
		p.lg.Info("Nothing to send, the accumulator records batch is empty")
		return nil
	}

	stream, err := p.remote.StreamBatchAppend(context.TODO(), &batch, p.callOpts...)
	if err != nil {
		p.lg.Error("Failed to send the stream batch records", zap.Error(err))
		return err
	}
	// Once we set the prevcallbacks, the current callbacks can be overwritten when we
	// send the doneSending signal to the accumulator. In this case the accumulator can continue
	// adding next record without blocking to wait for the whole stream receive operation
	// that is happening in parallel.
	p.prevcallbacks.setStore(p.currcallbacks.store)

	p.wait.Add(1)
	go func() {
		defer p.wait.Done()

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				p.streamRecvErrCh <- err
				// TODO: To Study
				// {"level":"error","ts":1712508800.4529178,"caller":"client/comlog.go:233",
				// "msg":"streamRecvErrCh","error":"rpc error: code = Internal desc = transport: SendHeader called multiple times","stacktrace":"github.com/ayoyu/comLog/client.(*comLogClient).sendBatch.func1\n\t/home/ayoub/Desktop/CommitLogProject/comLog/client/comlog.go:233"}
				// c.lg.Error("streamRecvErrCh", zap.Error(err))
				break
			}

			callback, ok := p.prevcallbacks.get(int(resp.Index))

			if ok {
				p.wait.Add(1)
				go callback(resp, p.wait)
			}
		}
	}()

	return nil
}

func (p *asyncProducer) sendLoop() {
	lingerTimer := time.NewTimer(p.batchOpt.linger)
	defer lingerTimer.Stop()

	for {
		select {
		case err := <-p.streamRecvErrCh:
			if p.enableReturnProducerErr {
				p.producerErr <- err
			}

		case closeErrCh := <-p.closeCh:
			p.lg.Info("Closing the async producer. Sending the remaining records from the accumulator "+
				"and waiting for all the current operations to finish...",
				zap.Int64("remaining records number", p.accumulator.recordsSize()))
			finalErr := p.sendBatch()
			// We don't really need to reset the accumulator next-positions as we are closing.
			p.wait.Wait()
			_ = p.lg.Sync() // TODO: handle the error

			closeErrCh <- finalErr
			return

		case <-p.accumulator.startSending():
			p.lg.Info("Accumulator record buffer is full. Start sending the batch records...")
			if err := p.sendBatch(); p.enableReturnProducerErr && err != nil {
				p.producerErr <- err
			}

			p.accumulator.doneSending() <- struct{}{}

		case <-lingerTimer.C:
			p.lg.Info("Wait linger time is triggered. Start sending the batch records...",
				zap.Duration("Wait linger time", p.batchOpt.linger))
			if err := p.sendBatch(); p.enableReturnProducerErr && err != nil {
				p.producerErr <- err
			}

			p.accumulator.resetNextOffsetAndIndex()
			lingerTimer.Reset(p.batchOpt.linger)
		}
	}
}

func (p *asyncProducer) Send(ctx context.Context, record *Record, callback OnCompletionSendCallback) error {
	IndexPos, err := p.accumulator.append(ctx, record.Data)

	if err == nil {
		p.currcallbacks.put(IndexPos, callback)
	}

	return err
}
