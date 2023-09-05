package client

import (
	"context"
	"sync"

	pb "github.com/ayoyu/comLog/api"
	"google.golang.org/grpc"
)

type (
	// Record represents the record data in bytes to send to the log server to be appended.
	Record pb.Record

	// Offset represents the integer offset with which we can read a record from the log server.
	Offset pb.Offset

	// BatchRecords represents a batch of `Record` to send.
	BatchRecord pb.BatchRecords

	// AppendRecordResp represents the server response from appending a record.
	// The `offset` indicates the offset at which the record was assigned in the log
	// while the `nbrOfStoredBytes` indicates the number of bytes that were stored in the log.
	AppendResponse pb.AppendRecordResp

	// BatchAppendResp represents the server response from appending a batch of records in the same rpc call.
	BatchAppendResponse pb.BatchAppendResp

	// ReadRecordResp represents the server response from reading a record.
	// The `record` holds the actual record in bytes while the `nbrOfReadBytes` indicates
	// the number of bytes that we were able to read.
	ReadResponse pb.ReadRecordResp
)

// OnCompletionSendCallback is a callback function to be invoked when the asynchronous send operation is done.
// Both the append response `AppendResponse` and the `err` error are the results from the rpc send operation.
type OnCompletionSendCallback func(resp *AppendResponse, err error)

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
	// records into requests and transmitting them the remote log server, for this reason the callback should be fast
	// enough to not delay the sending of messages to the server from other goroutines.
	Send(ctx context.Context, record *Record, callback OnCompletionSendCallback)

	// BatchAppend synchronously sends a batch of records to the remote log server.
	// This operation will block waiting for the `BatchAppendResponse` response from the server which represents
	// a slice of `pb.BatchAppendResp_RespWithOrder` containing the offset the record was assigned to, the number of
	// bytes stored for each record and the original index of the record inside the batch to identify the record that
	// was succefully appended since the batch append operation on the server side is asynchronous.
	BatchAppend(ctx context.Context, records *BatchRecord) (*BatchAppendResponse, error)

	// Read synchronously reads the record corresponding to the given offset. This operation will block
	// waiting for the `ReadResponse` response from the server which includes the recod in bytes `[]byte`
	// and the number of bytes we were able to read.
	Read(ctx context.Context, offset *Offset) (*ReadResponse, error)
}

type comLogClient struct {
	remote   pb.ComLogRpcClient
	callOpts []grpc.CallOption

	mu          sync.Mutex // will see if we need this mutex or not
	pendingFifo []*Record
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

func (c *comLogClient) Send(ctx context.Context, record *Record, callback OnCompletionSendCallback) {
	// TODO
}

func (c *comLogClient) BatchAppend(ctx context.Context, records *BatchRecord) (*BatchAppendResponse, error) {
	// TODO
	return nil, nil
}

func (c *comLogClient) Read(ctx context.Context, offset *Offset) (*ReadResponse, error) {
	pb_in := (*pb.Offset)(offset)

	pb_out, err := c.remote.Read(ctx, pb_in, c.callOpts...)
	if err != nil {
		return nil, err
	}

	return (*ReadResponse)(pb_out), nil
}
