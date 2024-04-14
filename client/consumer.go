package client

import (
	"context"
	"time"

	pb "github.com/ayoyu/comLog/api"
	"google.golang.org/grpc"
)

type (
	// Offset represents the integer offset with which we can read a record from the log server.
	Offset = pb.Offset

	// ReadRecordResp represents the server response from reading a record.
	// The `record` holds the actual record in bytes while the `nbrOfReadBytes` indicates
	// the number of bytes that we were able to read.
	ReadResponse = pb.ReadRecordResp
)

type Consumer interface {
	// Read synchronously reads the record corresponding to the given offset. This operation will block
	// waiting for the `ReadResponse` response from the server which includes the recod in bytes `[]byte`
	// and the number of bytes we were able to read.
	Read(ctx context.Context, offset *Offset) (*ReadResponse, error)

	// Overrides the fetch offsets that the consumer will use on the next `Poll(timeout)`.
	// The next Consumer Record which will be retrieved when `Poll(timeout)` is invoked will have the offset specified,
	// given that a record with that offset exists.
	//
	// Seeking to the offset smaller than the log start offset or larger than the log end offset means an invalid offset
	// is reached. When we hit this case, the invalid offset behaviour is controlled by the `Client.WithAutoOffsetResetProperty`.
	// If this is set to "earliest", the next poll will return records from the starting offset. If it is set to "latest",
	// it will seek to the last offset (similar to `seekToEnd()`). If it is set to "nil", an grpc `ErrOffsetOutOfRange`
	// will be returned.
	Seek(offset *Offset) error
	SeekToEnd()
	SeekToBegining()

	// On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially.
	// The last consumed offset can be manually set through `Seek(offset *Offset)` or automatically with
	// "Automatic Offset Committing (enabled by default)" set as the last committed offset.
	// See Automatic Offset Committing Vs Manual Offset Control
	//
	// TODO: It should return a slice of `[]ConsumerMessage` where `ConsumerMessage` contains the record and the offset.
	Poll(timeout time.Duration)

	// Synchronously commit offsets returned on the last `Poll(timeout)`. (See also `commitAsync(OffsetCommitCallback)`)
	CommitSync(timeout time.Duration) error
}

// TODO: Implements the Consumer Interface.
type consumer struct {
	remote   pb.ComLogRpcClient
	callOpts []grpc.CallOption
}

func NewConsumer(c *Client) *consumer {
	return &consumer{
		remote:   pb.NewComLogRpcClient(c.conn),
		callOpts: c.callOpts,
	}
}

func (p *consumer) Read(ctx context.Context, offset *Offset) (*ReadResponse, error) {
	pb_out, err := p.remote.Read(ctx, offset, p.callOpts...)
	if err != nil {
		return nil, err
	}

	return pb_out, nil
}
