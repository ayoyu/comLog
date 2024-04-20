package client

import (
	"math"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TODO: Needs to figure out some of the default values
const (
	defaultMaxAttemps        = 4
	defaultInitialBackoff    = ".01s"
	defaultMaxBackoff        = ".01s"
	defaultBackoffMultiplier = 1.

	mainRpcServiceName = "comLog.api.ComLogRpc"

	defaultmaxSendMsgSize = 2 * 1024 * 1024

	// 16*1024 similar to the default in kafka Producer.
	// Ref: https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html#batch-size
	defaultBatchSize = 16384
)

// Defaults gRPC Dial Options
var (
	defaultServiceConfigRawJSON = `{
  "methodConfig": [
    {
      "name": [
        {
          "service": "comLog.api.ComLogRpc"
        }
      ],
      "retryPolicy": {
        "MaxAttempts": 4,
        "InitialBackoff": ".01s",
        "MaxBackoff": ".01s",
        "BackoffMultiplier": 1,
        "RetryableStatusCodes": [
          "UNAVAILABLE",
		  "RESOURCE_EXHAUSTED",
		  "CANCELLED"
        ]
      }
    }
  ]
}`

	defaultRetryableStatusCodes = []RetryableStatus{
		UNAVAILABLE,
		RESOURCE_EXHAUSTED,
		CANCELLED,
	}

	defaultServiceConfig = grpc.WithDefaultServiceConfig(defaultServiceConfigRawJSON)

	defaultTLSInsecureCreds = grpc.WithTransportCredentials(insecure.NewCredentials())
)

// Defaults gRPC Call Options
var (
	// Client-side handling retrying of request failures where data was not written to the wire or
	// where server indicates it did not process the data.
	// If waitForReady is false and the connection is in the TRANSIENT_FAILURE state, the RPC will fail immediately.
	// Otherwise, the RPC client will block the call until a connection is available (or the call is canceled or times out)
	// and will retry the call if it fails due to a transient error.
	// gRPC default is "WaitForReady(false)", we choose default to true to minimize client request error responses
	// due to transient failure. Ref: https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md
	defaultWaitForReady = grpc.WaitForReady(true)

	// Client-side request send limit in bytes.
	// Make sure that "client-side send limit < server-side default send/recv limit".
	// gRPC default is math.MaxInt32. Our default is 2MB as the max Record.Data size to Append.
	//
	// The value of `defaultmaxSendMsgSize` < `server.DefaultMaxRecvMsgSizeBytes`
	defaultMaxCallSendMsgSize = grpc.MaxCallSendMsgSize(defaultmaxSendMsgSize)

	// Client-side response receive limit in bytes.
	// Make sure that "client-side receive limit >= server-side default send/recv limit"
	// because range response can easily exceed request send limits
	// Default to math.MaxInt32; writes exceeding server-side send limit fails anyway.
	// gRPC default is 4MB
	defaultMaxCallRecvMsgSize = grpc.MaxCallRecvMsgSize(math.MaxInt32)
)

// The name of the RPC status code.
// Ref: https://github.com/googleapis/googleapis/blob/master/google/rpc/code.proto.
type RetryableStatus string

// TODO: Needs to figure out what's to define as a retryable status in our context
const (
	CANCELLED           RetryableStatus = "CANCELLED"
	UNKNOWN             RetryableStatus = "UNKNOWN"
	INVALID_ARGUMENT    RetryableStatus = "INVALID_ARGUMENT"
	DEADLINE_EXCEEDED   RetryableStatus = "DEADLINE_EXCEEDED"
	NOT_FOUND           RetryableStatus = "NOT_FOUND"
	ALREADY_EXISTS      RetryableStatus = "ALREADY_EXISTS"
	PERMISSION_DENIED   RetryableStatus = "PERMISSION_DENIED"
	UNAUTHENTICATED     RetryableStatus = "UNAUTHENTICATED"
	RESOURCE_EXHAUSTED  RetryableStatus = "RESOURCE_EXHAUSTED"
	FAILED_PRECONDITION RetryableStatus = "FAILED_PRECONDITION"
	ABORTED             RetryableStatus = "ABORTED"
	OUT_OF_RANGE        RetryableStatus = "OUT_OF_RANGE"
	UNIMPLEMENTED       RetryableStatus = "UNIMPLEMENTED"
	INTERNAL            RetryableStatus = "INTERNAL"
	UNAVAILABLE         RetryableStatus = "UNAVAILABLE"
	DATA_LOSS           RetryableStatus = "DATA_LOSS"
)

// RetryOptionParameters specifies the retry policy configuration parameters.
// Ref: https://github.com/grpc/grpc-proto/blob/cdd9ed5c3d3f87aef62f373b93361cf7bddc620d/grpc/service_config/service_config.proto
type RetryOptionParameters struct {
	// The maximum number of RPC attempts, including the original attempt (Exponential backoff parameter).
	MaxAttemps int `json:"MaxAttemps"`
	// The initial retry delay (Exponential backoff parameter).
	// The value of this field represents a duration string in decimal number of seconds that maps to google.protobuf.Duration.
	// Examples: 3 seconds and 1 nanosecond should be expressed as "3.000000001s",
	// 3 seconds and 1 microsecond should be expressed as "3.000001s".
	// 3 seconds with 0 nanoseconds should be expressed as "3s".
	// Reference: https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/duration.proto
	InitialBackoff string `json:"InitialBackoff"`
	// The maximum retry delay (Exponential backoff parameter)
	MaxBackoff string `json:"MaxBackoff"`
	// The rate with which we multiply the initial delay to get the target delay.
	// For example, the nth attempt will occur at random(0, min(initial_backoff*backoff_multiplier**(n-1), max_backoff)).
	BackoffMultiplier float64 `json:"BackoffMultiplier"`
	// The RPC status code that allow a retry.
	RetryableStatusCodes []RetryableStatus `json:"RetryableStatusCodes"`
}

// setDefaultRetryPolicy sets the default value for the retry option fields if they are not set.
func setDefaultRetryPolicy(cfg *RetryOptionParameters) {
	if cfg.MaxAttemps == 0 {
		cfg.MaxAttemps = defaultMaxAttemps
	}
	if cfg.InitialBackoff == "" {
		cfg.InitialBackoff = defaultInitialBackoff
	}
	if cfg.MaxBackoff == "" {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	if cfg.BackoffMultiplier == 0 {
		cfg.BackoffMultiplier = defaultBackoffMultiplier
	}
	if len(cfg.RetryableStatusCodes) == 0 {
		cfg.RetryableStatusCodes = defaultRetryableStatusCodes
	}
}

type rpcServiceConfig struct {
	Service string `json:"service"`
	// Currently we don't need to specify the method.
	// Method  string `json:"method"`
}

type methodConfig struct {
	Name []rpcServiceConfig `json:"name"`

	*RetryOptionParameters
}

type serviceConfig struct {
	MethodCfg []*methodConfig `json:"methodConfig"`
}

type retryOption struct {
	serviceCfgRawJSON string
}

type authOption struct {
	username string
	password string
}

type tlsOption struct {
	creds credentials.TransportCredentials
}

// TODO: This options must be seperated btw the producer/consumer options.
type callOption struct {
	// maxCallSendMsgSize and maxCallRecvMsgSize are used to set the req/resp size limit
	maxCallSendMsgSize int
	maxCallRecvMsgSize int
}

type dialOption struct {
	dialTimeout time.Duration
	dialBlock   bool
}

type keepAliveProbeOption struct {
	dialKeepAliveTime    time.Duration
	dialKeepAliveTimeout time.Duration
	permitWithoutStream  bool
	// blocking dial
	dialBlock bool
}

type asyncProducerOptions struct {
	batch struct {
		// The upper bound of the batch size to use for buffering records to be sent later.
		// Default to 16384=16*1024
		batchSize int
		// The upper bound of time awaiting for other records to show up to fill the buffer in order
		// to batch the maximum possible
		linger time.Duration
	}

	returnErrors struct {
		enabled bool
	}
}

type ResetOffset uint

const (
	OldestOffset ResetOffset = iota
	NewestOffset
)

type consumerOptions struct {
	offsetsAutoCommit struct {
		enabled  bool
		interval time.Duration
	}

	autoOffsetResetProperty struct {
		// The initial offset to use when reset happen. It should be either
		// NewestOffset or OldestOffset. Defaults to NewestOffset.
		initial ResetOffset
	}
}

type options struct {
	retryOpt retryOption
	authOpt  authOption
	tlsOpt   tlsOption
	callOpt  callOption
	dialOpt  dialOption
	aliveOpt keepAliveProbeOption

	asyncProducerOpts asyncProducerOptions
	consumerOpts      consumerOptions
}
