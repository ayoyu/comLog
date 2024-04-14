package client

import (
	"context"

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
}

type consumer struct {
	remote   pb.ComLogRpcClient
	callOpts []grpc.CallOption
}

func NewConsumer(c *Client) Consumer {
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
