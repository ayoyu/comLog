package client

import (
	"context"

	pb "github.com/ayoyu/comLog/api"
	"google.golang.org/grpc"
)

type (
	// BatchRecords represents a batch of `Record` to send.
	BatchRecord = pb.BatchRecords

	// AppendRecordResp represents the server response from appending a record.
	// The `offset` indicates the offset at which the record was assigned in the log
	// while the `nbrOfStoredBytes` indicates the number of bytes that were stored in the log.
	AppendResponse = pb.AppendRecordResp

	// BatchAppendResp represents the server response from appending a batch of records in the same rpc call.
	BatchAppendResponse = pb.BatchAppendResp
)

type SyncProducer interface {
	// Append synchronously sends a record to the remote log server. This operation will block waiting
	// for the `AppendResponse` response from the server which specifies the offset to which the
	// record was assigned and the number of bytes stored
	Append(ctx context.Context, record *Record) (*AppendResponse, error)

	// BatchAppend synchronously sends a batch of records to the remote log server.
	// This operation will block waiting for the `BatchAppendResponse` response from the server which represents
	// a slice of `pb.BatchAppendResp_RespWithOrder` containing the offset the record was assigned to, the number of
	// bytes stored for each record and the original index of the record inside the batch to identify the record that
	// was succefully appended since the batch append operation on the server side is asynchronous.
	//
	// Note that appending records from the batch can succeed and fail individually; if some succeed and some fail,
	// the BatchAppend will stop and return the records that have been successfully stored so far with their original
	// indexes in the batch and the error that caused the shutdown, which means in the worst case scenario where no record
	// was successfully appended we will have a `BatchAppendResponse` with an empty slice and the first encountered error
	// that caused the shutdown.
	BatchAppend(ctx context.Context, batch *BatchRecord) (*BatchAppendResponse, error)
}

func NewSyncProducer(c Client) SyncProducer {
	p := &syncProducer{
		remote:   pb.NewComLogRpcClient(c.RpcConnection()),
		callOpts: c.RpcCallOptions(),
	}

	return p
}

type syncProducer struct {
	remote   pb.ComLogRpcClient
	callOpts []grpc.CallOption
}

func (p *syncProducer) Append(ctx context.Context, record *Record) (*AppendResponse, error) {
	pb_out, err := p.remote.Append(ctx, record, p.callOpts...)
	if err != nil {
		return nil, err
	}

	return pb_out, nil
}

func (p *syncProducer) BatchAppend(ctx context.Context, batch *BatchRecord) (*BatchAppendResponse, error) {
	return p.remote.BatchAppend(ctx, batch, p.callOpts...)
}
