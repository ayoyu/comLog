package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/ayoyu/comLog/api"
	"github.com/ayoyu/comLog/comLog"
)

type Config struct {
	LogCfg comLog.Config

	Lg *zap.Logger
}

type ComLogServer struct {
	pb.UnimplementedComLogRpcServer
	Config

	log        *comLog.Log
	shutDownCh chan os.Signal
}

func NewComLogServer(cfg Config) (*ComLogServer, error) {
	log, err := comLog.NewLog(cfg.LogCfg)
	if err != nil {
		return nil, err
	}

	shutDownCh := make(chan os.Signal, 1)
	signal.Notify(
		shutDownCh,
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGINT,
	)

	if cfg.Lg == nil {
		cfg.Lg = zap.NewNop()
	}

	s := &ComLogServer{
		Config:     cfg,
		log:        log,
		shutDownCh: shutDownCh,
	}
	s.Lg.Info("Initializing the commit log server", zap.String("data directory", s.log.Data_dir))

	return s, nil
}

func (s *ComLogServer) Close() error {
	s.Lg.Info("Closing the commit log server", zap.String("data directory", s.log.Data_dir))

	return s.log.Close()
}

func (s *ComLogServer) GracefulShutdown(grpcGracefulStop func()) <-chan struct{} {
	shutDownDone := make(chan struct{})

	go func() {
		sig := <-s.shutDownCh
		s.Lg.Info("Shutdown gracefully the commit log server", zap.Any("signal", sig))

		var wait sync.WaitGroup
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := s.Close(); err != nil {
				s.Lg.Warn("Closing the commit log failed", zap.Error(err))
				return
			}

			s.Lg.Info("Closing the commit log operation is done")
		}()

		wait.Add(1)
		go func() {
			grpcGracefulStop()
			s.Lg.Info("Grpc server GracefulStop operation is done")

			wait.Done()
		}()

		wait.Wait()
		close(shutDownDone)
	}()

	return shutDownDone
}

func (s *ComLogServer) Append(ctx context.Context, record *pb.Record) (*pb.AppendRecordResp, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	offset, nn, err := s.log.Append(record.Data)
	if err != nil {
		return nil, err
	}

	return &pb.AppendRecordResp{
		Offset:           offset,
		NbrOfStoredBytes: int64(nn),
	}, nil
}

func (s *ComLogServer) BatchAppend(ctx context.Context, records *pb.BatchRecords) (*pb.BatchAppendResp, error) {
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	type callResp struct {
		resp *pb.BatchAppendResp_RespWithOrder
		err  error
	}
	resCh := make(chan callResp, len(records.Batch))

	for i := 0; i < len(records.Batch); i++ {
		go func(cxt context.Context, i int) {
			select {
			case <-ctx.Done():
				return

			default:
				offset, nn, err := s.log.Append(records.Batch[i].Data)
				select {
				case <-ctx.Done():
					return
				case resCh <- callResp{
					resp: &pb.BatchAppendResp_RespWithOrder{
						Resp: &pb.AppendRecordResp{
							Offset:           offset,
							NbrOfStoredBytes: int64(nn),
						},
						Index: int64(i),
					},
					err: err,
				}:
				}
			}
		}(ctx, i)
	}

	results := make([]*pb.BatchAppendResp_RespWithOrder, 0, len(records.Batch))
	var err error

Loop:
	for i := 0; i < len(records.Batch); i++ {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			break Loop

		case res := <-resCh:
			if res.err != nil {
				// In case if any error we break and we cancel the other goroutines with the defered `cancelFunc`
				err = fmt.Errorf("failed appending record at index %d. Original error: %w", res.resp.Index, res.err)
				break Loop
			}
			results = append(results, res.resp)
		}
	}

	return &pb.BatchAppendResp{Response: results}, err
}

func (s *ComLogServer) StreamBatchAppend(records *pb.BatchRecords, stream pb.ComLogRpc_StreamBatchAppendServer) error {
	errCh := make(chan error, len(records.Batch))
	done := make(chan struct{})

	for i := 0; i < len(records.Batch); i++ {
		go func(i int) {
			var (
				offset uint64
				nn     int
				err    error
				errMsg string
			)
			select {
			case <-done:
				return
			default:
				offset, nn, err = s.log.Append(records.Batch[i].Data)
			}
			s.Lg.Sugar().Debugf("Server: ", string(records.Batch[i].Data), i, offset)

			if err != nil {
				errMsg = err.Error()
			}

			resp := pb.StreamAppendRecordResp{
				Resp: &pb.AppendRecordResp{
					Offset:           offset,
					NbrOfStoredBytes: int64(nn),
				},
				Index:    int64(i),
				ErrorMsg: errMsg,
			}

			select {
			case <-done:
				return
			default:
				err = stream.Send(&resp)
				errCh <- err
			}

		}(i)
	}

	for i := 0; i < len(records.Batch); i++ {
		err := <-errCh
		if err != nil {
			close(done)
			return err
		}
	}

	return nil
}

func (s *ComLogServer) Read(ctx context.Context, offset *pb.Offset) (*pb.ReadRecordResp, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	nn, record, err := s.log.Read(offset.Value)
	if err != nil {
		if errors.Is(err, comLog.SegOutOfRangeError) || errors.Is(err, comLog.IndexOutOfRangeError) {
			return nil, status.Errorf(codes.OutOfRange, err.Error())
		}

		if errors.Is(err, comLog.InvalidOffsetArgError) {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		return nil, err
	}

	return &pb.ReadRecordResp{
		Record:         record,
		NbrOfReadBytes: int64(nn),
	}, nil
}

func (s *ComLogServer) Flush(ctx context.Context, typ *pb.IndexFlushSyncType) (*emptypb.Empty, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// mmap sync is the default
	return nil, s.log.Flush(comLog.IndexSyncType(typ.Value))
}

func (s *ComLogServer) GetMetaData(context.Context, *emptypb.Empty) (*pb.LogMetaData, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetMetaData not implemented")
}

func (s *ComLogServer) CollectSegments(context.Context, *pb.CollectOffset) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CollectSegments not implemented")
}
