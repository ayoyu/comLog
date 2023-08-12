package client

import (
	"context"

	pb "github.com/ayoyu/comLog/api"
	"google.golang.org/grpc"
)

type (
	// in(s) Request
	Record pb.Record
	Offset pb.Offset

	// out(s) Request
	AppendResponse pb.AppendRecordResp
	ReadResponse   pb.ReadRecordResp
)

type ComLogClient interface {
	Append(ctx context.Context, record *Record) (*AppendResponse, error)
	Read(ctx context.Context, offset *Offset) (*ReadResponse, error)
}

type comLogClient struct {
	remote   pb.ComLogRpcClient
	callOpts []grpc.CallOption
}

func NewClientComLog(c *Client) ComLogClient {
	return &comLogClient{
		remote:   pb.NewComLogRpcClient(c.conn),
		callOpts: c.callOpts,
	}
}

func (c *comLogClient) Append(ctx context.Context, record *Record) (*AppendResponse, error) {
	pb_in := (*pb.Record)(record)

	pb_out, err := c.remote.Append(ctx, pb_in, c.callOpts...)
	if err != nil {
		return nil, err
	}

	return (*AppendResponse)(pb_out), nil
}

func (c *comLogClient) Read(ctx context.Context, offset *Offset) (*ReadResponse, error) {
	pb_in := (*pb.Offset)(offset)

	pb_out, err := c.remote.Read(ctx, pb_in, c.callOpts...)
	if err != nil {
		return nil, err
	}

	return (*ReadResponse)(pb_out), nil
}
