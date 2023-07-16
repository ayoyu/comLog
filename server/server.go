package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"

	pb "github.com/ayoyu/comLog/api"
	"github.com/ayoyu/comLog/comLog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	OPT_TLS              = flag.Bool("tls", false, "(Optional) Connection uses TLS if true, else plain TCP")
	OPT_CERT_FILE        = flag.String("cert_file", "", "(Optional) The TLS cert file if tls is true")
	OPT_KEY_FILE         = flag.String("key_file", "", "(Optional) The TLS key file if tls is true")
	OPT_PORT             = flag.Int("port", 50052, "(Optional) The server port")
	OPT_LOG_NBR_SEGMENTS = flag.Int("log_nbr_of_segments", 0, "(Optional) The number of segments from an existing log data file system directory")

	LOG_DATA_DIR        = flag.String("log_data_dir", "", "The log data file system directory")
	LOG_STORE_MAX_BYTES = flag.Uint64("log_store_max_bytes", 0, "The log store max bytes")
	LOG_INDEX_MAX_BYTES = flag.Uint64("log_index_max_bytes", 0, "The log index max bytes")
)

type ComLogServer struct {
	pb.UnimplementedComLogRpcServer
	log        *comLog.Log
	shutDownCh chan os.Signal
}

func NewComLogServer(conf comLog.Config) (*ComLogServer, error) {
	log, err := comLog.NewLog(conf)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Initializing the commit log server (data directory: %s)", conf.Data_dir)

	shutDownCh := make(chan os.Signal, 1)
	signal.Notify(
		shutDownCh,
		os.Interrupt,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGINT,
	)

	return &ComLogServer{
		log:        log,
		shutDownCh: shutDownCh,
	}, nil
}

func (s *ComLogServer) close() error {
	logrus.Infof("Start closing the commit log server (data directory: %s)", s.log.Data_dir)

	return s.log.Close()
}

func (s *ComLogServer) gracefulShutdown(grpcGracefulStop func()) <-chan struct{} {
	shutDownDone := make(chan struct{})

	go func() {
		sig := <-s.shutDownCh
		logrus.Infof("Received signal %v, attempting to gracefully shutdown the commit log server", sig)

		var wait sync.WaitGroup
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := s.close(); err != nil {
				logrus.Warnf("Closing the commit log failed with error %v", err)
				return
			}
			logrus.Infof("Closing the commit log operation finished")
		}()

		wait.Add(1)
		go func() {
			grpcGracefulStop()
			logrus.Infof("Grpc server GracefulStop operation finished")
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

func (s *ComLogServer) Read(ctx context.Context, offset *pb.Offset) (*pb.ReadRecordResp, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	nn, record, err := s.log.Read(offset.Value)
	if err != nil {
		return nil, err
	}

	return &pb.ReadRecordResp{
		Record:         record,
		NbrOfReadBytes: int64(nn),
	}, nil
}

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *OPT_PORT))
	if err != nil {
		logrus.Fatalf("Failed to start listening on port %d, %v\n", *OPT_PORT, err)
	}

	var opt []grpc.ServerOption
	if *OPT_TLS {
		if *OPT_CERT_FILE == "" || *OPT_KEY_FILE == "" {
			logrus.Warnf("The cert_file or key_file flags are empty while the tls option is true. " +
				"The server in this case will start without any TLS configuration")

		} else {
			cred, err := credentials.NewServerTLSFromFile(*OPT_CERT_FILE, *OPT_KEY_FILE)
			if err != nil {
				logrus.Fatalf("Failed to generate TLS credentials %v\n", err)
			}
			opt = append(opt, grpc.Creds(cred))
		}
	}

	grpcServer := grpc.NewServer(opt...)

	comlogServer, err := NewComLogServer(
		comLog.Config{
			Data_dir:      *LOG_DATA_DIR,
			NbrOfSegments: *OPT_LOG_NBR_SEGMENTS,
			StoreMaxBytes: *LOG_STORE_MAX_BYTES,
			IndexMaxBytes: *LOG_INDEX_MAX_BYTES,
		},
	)
	if err != nil {
		logrus.Fatalf("Failed to init the commit log %v\n", err)
	}

	pb.RegisterComLogRpcServer(grpcServer, comlogServer)

	shutDownDone := comlogServer.gracefulShutdown(grpcServer.GracefulStop)

	logrus.Infof("Start listening on port %d\n", *OPT_PORT)
	if err := grpcServer.Serve(lis); err != nil {
		logrus.Fatalf("Failed to start the grpc server %v\n", err)
	}

	<-shutDownDone
}
