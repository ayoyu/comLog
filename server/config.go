package server

import (
	"math"
	"time"
)

const (
	DefaultMaxRecvMsgSizeBytes = 4 * 1024 * 1024 // gRPC default to 4 MB

	DefaultMaxConcurrentStreams = math.MaxUint32

	DefaultGRPCEnforceKeepAliveMinTime     = 5 * time.Second  //  gRPC default is 5 minutes
	DefaultGRPCKeepAliveInterval           = 2 * time.Hour    // gRPC default is 2 hours
	DefaultGRPCKeepAliveTimeout            = 20 * time.Second // gRPC default is 20 seconds
	DefaultGRPCKeepAlivePermitWithotStream = false            // grpc default is false

	DefaultCommitLogStoreMaxBytes = 4 * 1024 * 1024
	// 16 bytes is the indexWidth (= offsetWidth + positionWidth)
	// 4 MB in store corresponds to 256 KB in index
	DefaultCommitLogIndexMaxBytes = DefaultCommitLogStoreMaxBytes / 16
)
