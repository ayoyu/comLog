package main

import (
	"flag"
	"fmt"
	"net"

	pb "github.com/ayoyu/comLog/api"
	"github.com/ayoyu/comLog/comLog"
	"github.com/ayoyu/comLog/server"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

var (
	// grpc secure options
	OPT_TLS       = flag.Bool("tls", false, "(Optional) Connection uses TLS if true, else plain TCP.")
	OPT_CERT_FILE = flag.String("cert-file", "", "(Optional) The TLS cert file if tls is true.")
	OPT_KEY_FILE  = flag.String("key-file", "", "(Optional) The TLS key file if tls is true.")

	// grpc call options
	OPT_MAX_REQ_BYTES = flag.Int("max-request-bytes", server.DefaultMaxRequestBytes, "(Optional) Maximum client request size in bytes the server will accept.")

	// grpc keep alive options
	OPT_GRPC_ENFORCE_KEEP_ALIVE_MIN_TIME      = flag.Duration("grpc-keepalive-enforce-min-time", server.DefaultGRPCEnforceKeepAliveMinTime, "(Optional) Minimum interval duration that a client should wait before pinging server.")
	OPT_GRPC_KEEP_ALIVE_PERMIT_WITHOUT_STREAM = flag.Bool("grpc-keepalive-permit-without-stream", server.DefaultGRPCKeepAlivePermitWithotStream, "(Optional) If true, it will permit the client to send pings even when there are no active streams.")
	OPT_GRPC_KEEP_ALIVE_INTERVAL              = flag.Duration("grpc-keepalive-interval", server.DefaultGRPCKeepAliveInterval, "(Optional) Frequency duration of server-to-client ping to check if a connection is alive from the client side.")
	OPT_GRPC_KEEP_ALIVE_TIMEOUT               = flag.Duration("grpc-keepalive-timeout", server.DefaultGRPCKeepAliveTimeout, "(Optional) Additional duration of wait before closing a non-responsive connection.")

	OPT_MAX_CONCURRENT_STREAMS = flag.Int("max-concurrent-streams", server.DefaultMaxConcurrentStreams, "(Optional) Maximum concurrent streams that each client can open at a time.")

	// commit log config
	LOG_DATA_DIR            = flag.String("log-data-dir", "", "The log data file system directory")
	OPT_LOG_STORE_MAX_BYTES = flag.Uint64("log-store-max-bytes", server.DefaultCommitLogStoreMaxBytes, "(Optional) The log store max bytes")
	OPT_LOG_INDEX_MAX_BYTES = flag.Uint64("log-index-max-bytes", server.DefaultCommitLogIndexMaxBytes, "(Optional) The log index max bytes")
	OPT_LOG_NBR_SEGMENTS    = flag.Int("log-segments-number", 0, "(Optional) The number of segments from an existing log data file system directory.")

	OPT_PORT = flag.Int("port", 50052, "(Optional) The server port.")
)

func newGrpcServer() (*grpc.Server, error) {
	flag.Parse()
	var opts []grpc.ServerOption

	if *OPT_TLS {
		if *OPT_CERT_FILE == "" || *OPT_KEY_FILE == "" {
			logrus.Warnf("The cert_file or key_file flags are empty while the tls boolean flag is true. " +
				"The server in this case will start without any TLS configuration.")
		} else {
			cred, err := credentials.NewServerTLSFromFile(*OPT_CERT_FILE, *OPT_KEY_FILE)
			if err != nil {
				return nil, err
			}
			opts = append(opts, grpc.Creds(cred))
		}
	}

	opts = append(opts,
		grpc.KeepaliveEnforcementPolicy(
			keepalive.EnforcementPolicy{
				// gRPC default is 5 minutes
				MinTime: *OPT_GRPC_ENFORCE_KEEP_ALIVE_MIN_TIME,
				// Default to false. If false and client sends keepalive pings when there are no active streams(RPCs)
				// the server will repond GOAWAY and close the connection.
				PermitWithoutStream: *OPT_GRPC_KEEP_ALIVE_PERMIT_WITHOUT_STREAM,
			},
		),
		grpc.KeepaliveParams(
			keepalive.ServerParameters{
				// gRPC default is 2 hours
				Time: *OPT_GRPC_KEEP_ALIVE_INTERVAL,
				// gRPC default is 20 seconds
				Timeout: *OPT_GRPC_KEEP_ALIVE_TIMEOUT,
			},
		))

	grpcServer := grpc.NewServer(opts...)
	return grpcServer, nil
}

func main() {
	grpcServer, err := newGrpcServer()
	if err != nil {
		logrus.Fatalf("Failed to create the gRPC server %v\n", err)
	}
	comlogServer, err := server.NewComLogServer(
		comLog.Config{
			Data_dir:      *LOG_DATA_DIR,
			NbrOfSegments: *OPT_LOG_NBR_SEGMENTS,
			StoreMaxBytes: *OPT_LOG_STORE_MAX_BYTES,
			IndexMaxBytes: *OPT_LOG_INDEX_MAX_BYTES,
		},
	)
	if err != nil {
		logrus.Fatalf("Failed to init the commit log %v\n", err)
	}
	pb.RegisterComLogRpcServer(grpcServer, comlogServer)

	shutDownDone := comlogServer.GracefulShutdown(grpcServer.GracefulStop)

	// Start serving
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *OPT_PORT))
	if err != nil {
		logrus.Fatalf("Failed to start listening on port %d, %v\n", *OPT_PORT, err)
	}
	logrus.Infof("Start listening on port :%d\n", *OPT_PORT)

	if err := grpcServer.Serve(lis); err != nil {
		logrus.Fatalf("Failed to start the grpc server %v\n", err)
	}

	<-shutDownDone
}
