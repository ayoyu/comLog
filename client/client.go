package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type Client interface {
	// Close shuts down the commit log client's connections.
	// It is required to call this function before a client object passes out of scope,
	// as it will otherwise leak memory.
	// You must close any Producers or Consumers using a client BEFORE you close the client.
	Close() error

	// Options returns the options struct of the client.
	Options() *options

	// RpcConnection returns the grpc client connection used to establish the connection with the server.
	RpcConnection() *grpc.ClientConn

	// RpcCallOptions returns the grpc call option.
	RpcCallOptions() []grpc.CallOption

	// Logger returns the zap logger
	Logger() *zap.Logger
}

type Option func(*client) error

type client struct {
	*options
	serverAddr string
	context    context.Context
	cancel     context.CancelFunc
	dialOpts   []grpc.DialOption
	callOpts   []grpc.CallOption
	// The ClientConn contains one or more actual connections to real server backends and attempts
	// to keep these connections healthy by automatically reconnecting to them when they break.
	// Ref: https://github.com/grpc/grpc-go/blob/master/Documentation/anti-patterns.md
	conn *grpc.ClientConn

	lg *zap.Logger
}

func (c *client) Close() error {
	c.lg.Info("Closing the commit log client")
	c.cancel()
	if c.conn != nil {
		// TODO: wrap the grpc error
		return c.conn.Close()
	}

	return c.context.Err()
}

func (c *client) Options() *options {
	return c.options
}

func (c *client) RpcConnection() *grpc.ClientConn {
	return c.conn
}

func (c *client) RpcCallOptions() []grpc.CallOption {
	return c.callOpts
}

func (c *client) Logger() *zap.Logger {
	return c.lg
}

// WithLogger overrides the default no-op logger
func WithLogger(lg *zap.Logger) Option {
	return func(c *client) error {
		c.lg = lg
		return nil
	}
}

// WithRetryPolicyOption configures the retry policy option.
// Ref: https://github.com/grpc/grpc/blob/master/doc/service_config.md
func WithRetryPolicy(opt RetryOptionParameters) Option {
	return func(c *client) error {
		setDefaultRetryPolicy(&opt)

		m := methodConfig{
			Name: []rpcServiceConfig{
				{Service: mainRpcServiceName},
			},
			RetryOptionParameters: &opt,
		}
		srvcCfg := serviceConfig{
			MethodCfg: []*methodConfig{&m},
		}

		b, err := json.Marshal(&srvcCfg)
		if err != nil {
			return err
		}
		c.retryOpt.serviceCfgRawJSON = string(b)
		return nil
	}
}

// TODO
func WithAuth(username, password string) Option {
	return func(c *client) error {
		c.authOpt.username = username
		c.authOpt.password = password
		return nil
	}
}

// WithTLS configures the client-side tls configuration with the given root certificate authority certificate file
// to validate the server connection. The serverNameOverride is only for testing, if not empty it will override
// the virtual host name of authority in requests.
func WithTLS(rootCAFile, serverNameOverride string) Option {
	return func(c *client) error {
		creds, err := credentials.NewClientTLSFromFile(rootCAFile, serverNameOverride)
		if err != nil {
			return err
		}
		c.tlsOpt.creds = creds
		return nil
	}
}

// WithDialTimeout configures the timeout for failing to establish a dial connection with the server.
// This is valid if and only if WithBlock() is present, if dial is non-blocking the timeout will be ignored.
func WithDialTimeout(timeout time.Duration) Option {
	return func(c *client) error {
		c.dialOpt.dialTimeout = timeout
		return nil
	}
}

// WithDialBlock makes the Dial operation block until the
// underlying connection is up. Without this, Dial returns immediately and
// connecting the server happens in background.
//
// Use of this feature is not recommended. For more information, please see:
// https://github.com/grpc/grpc-go/blob/master/Documentation/anti-patterns.md
func WithDialBlock() Option {
	return func(c *client) error {
		c.dialOpt.dialBlock = true
		return nil
	}
}

// WithKeepAliveProbe configures the keep-alive-probe to ping the server for liveness.
// `dialTime` is the time after which client pings the server to see if transport is alive with `dialTimeout`
// which is the time that the client waits for a response for the keep-alive probe, if the response is not received
// in this time, the connection is closed. When `permitWithoutStream` is set to true the client will send keepalive pings
// to the server without any active streams(RPCs).
func WithKeepAliveProbe(dialTime, dialTimeout time.Duration, permitWithoutStream bool) Option {
	return func(c *client) error {
		c.aliveOpt.dialKeepAliveTime = dialTime
		c.aliveOpt.dialKeepAliveTimeout = dialTimeout
		c.aliveOpt.permitWithoutStream = permitWithoutStream
		return nil
	}
}

// WithMaxCallSendRecvMsgSize configures the client-side request/response send/receive limit size in bytes.
// The default values for `sendBytes` and `recvBytes` if they are not set are 2 MB (= 2 * 1024 * 1024) and
// `math.MaxInt32` respectively.
// Make sure that "client-side send limit < server-side default send/recv limit" and
// "client-side receive limit >= server-side default send/recv limit" because range response
// can easily exceed request send limits
//
// TODO: Since recently we seperated the client API into a producer/consumer API. We must also seperate this
// option btw the two. (The producer only sends and the consumer only receives). See sarama.config.Fetch
func WithMaxSendRecvMsgSize(sendBytes, recvBytes int) Option {
	return func(c *client) error {
		if recvBytes > 0 {
			if sendBytes == 0 && recvBytes < defaultmaxSendMsgSize {
				return fmt.Errorf(
					"if the send limit (%d bytes) is not defined, the recv limit (%d bytes) must be greater than 2MB (default)",
					sendBytes, recvBytes,
				)
			}

			if recvBytes < sendBytes {
				return fmt.Errorf(
					"gRPC message recv limit (%d bytes) must be greater than send limit (%d bytes)",
					recvBytes, sendBytes,
				)
			}
		}
		c.callOpt.maxCallSendMsgSize = sendBytes
		c.callOpt.maxCallRecvMsgSize = recvBytes
		return nil
	}
}

// WithAsyncProducerBatchSize configures the upper bound batch size to use for buffering with the async producer.
// The async producer will attempt to batch records together into fewer requests to help performance on both
// client and server.
// A small batch size will make buffering less common and so more requests to the server. If size is set to 0 this
// will entirely disable batching. On the opposite if the size is very large, this can be wastfull of memory as we
// will always allocate in advance a buffer with the specified size (You can choose to not set this option and work
// with the default size of 16384=16*1024 bytes)
//
// This size represents the upper limit of the batch size to send, i.e. if the buffer contains fewer records than
// this size, we will "linger" `WithLinger` while waiting for other records to appear and when the time comes,
// we will send the batch to the server even if there is still room for other records.
func WithAsyncProducerBatchSize(size int) Option {
	return func(c *client) error {
		c.asyncProducerOpts.batch.batchSize = size
		return nil
	}
}

// WithAsyncProducerLinger configures the upper bound awaiting time to wait for other records to show up in order
// to send them in a batch to the server with the async producer.
// This option is similar to `linger.ms` in Kafka Producer and also it is analogous to Nagle’s algorithm in TCP.
//
// This time represents the upper limit of waiting which means if the buffer is already full of records
// the batch will be sent immediately regardless of this option. The default is 0.
func WithAsyncProducerLinger(linger time.Duration) Option {
	return func(c *client) error {
		c.asyncProducerOpts.batch.linger = linger
		return nil
	}
}

// WithAsyncProducerReturnErrors enables (or not) receiving errors from the `Errors` channel when records failed
// to be delivered.
// If set to true, you MUST read from the respective channel `Errors` to prevent deadlock.
func WithAsyncProducerReturnErrors(enabled bool) Option {
	return func(c *client) error {
		c.asyncProducerOpts.returnErrors.enabled = enabled
		return nil
	}
}

// WithConsumerOffsetsAutoCommit enables The Automatic Offset Committing, i.e. whether or not to auto-commit updated
// offsets. `Interval` indicates how frequently to commit updated offsets. Ineffective unless auto-commit is enabled
// (default 1s)
func WithConsumerOffsetsAutoCommit(enabled bool, interval time.Duration) Option {
	return func(c *client) error {
		c.consumerOpts.offsetsAutoCommit.enabled = enabled
		c.consumerOpts.offsetsAutoCommit.interval = interval
		return nil
	}
}

// WithAutoOffsetResetProperty controlls the behavior when we `Seek` manually into an invalid offset, i.e. smaller than
// the log start or larger than the log end offsets.
// If this is set to `OldestOffset`, the next poll will return records from the starting offset.
// If it is set to `NewestOffset`, it will seek to the last offset.
func WithConsumerAutoOffsetResetProperty(resetOffset ResetOffset) Option {
	return func(c *client) error {
		if resetOffset > NewestOffset {
			return fmt.Errorf("invalid resetOffset argument, it must be `OldestOffset` or `NewestOffset`")
		}
		c.consumerOpts.autoOffsetResetProperty.initial = resetOffset
		return nil
	}
}

func (c *client) addDialOpts() {
	if c.retryOpt.serviceCfgRawJSON != "" {
		c.dialOpts = append(c.dialOpts, grpc.WithDefaultServiceConfig(c.retryOpt.serviceCfgRawJSON))
	} else {
		c.dialOpts = append(c.dialOpts, defaultServiceConfig)
	}

	if c.tlsOpt.creds != nil {
		c.dialOpts = append(c.dialOpts, grpc.WithTransportCredentials(c.tlsOpt.creds))
	} else {
		c.dialOpts = append(c.dialOpts, defaultTLSInsecureCreds)
	}

	if c.aliveOpt.dialKeepAliveTime > 0 {
		// TODO: define default probe values ?
		params := keepalive.ClientParameters{
			Time:                c.aliveOpt.dialKeepAliveTime,
			Timeout:             c.aliveOpt.dialKeepAliveTimeout,
			PermitWithoutStream: c.aliveOpt.permitWithoutStream,
		}
		c.dialOpts = append(c.dialOpts, grpc.WithKeepaliveParams(params))
	}

	if c.dialOpt.dialBlock {
		c.dialOpts = append(c.dialOpts, grpc.WithBlock())
	}
}

func (c *client) addCallOpts() {
	c.callOpts = append(c.callOpts, defaultWaitForReady)

	if c.callOpt.maxCallSendMsgSize > 0 {
		c.callOpts = append(c.callOpts, grpc.MaxCallSendMsgSize(c.callOpt.maxCallSendMsgSize))
	} else {
		c.callOpts = append(c.callOpts, defaultMaxCallSendMsgSize)
	}

	if c.callOpt.maxCallRecvMsgSize > 0 {
		c.callOpts = append(c.callOpts, grpc.MaxCallRecvMsgSize(c.callOpt.maxCallRecvMsgSize))
	} else {
		c.callOpts = append(c.callOpts, defaultMaxCallRecvMsgSize)
	}
}

func (c *client) dial() (*grpc.ClientConn, error) {
	dctx := c.context
	if c.dialOpt.dialTimeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(c.context, c.dialOpt.dialTimeout)
		defer cancel()
	}

	return grpc.DialContext(dctx, c.serverAddr, c.dialOpts...)
}

// Creates new commit log gRPC client with the given server address and options.
// The context is the default client context, it can be used to cancel grpc dial
// out and other operations that do not have an explicit context.
func New(ctx context.Context, serverAddr string, opts ...Option) (*client, error) {
	baseCtx := context.TODO()
	if ctx != nil {
		baseCtx = ctx
	}
	ctx, cancel := context.WithCancel(baseCtx)

	cli := &client{
		serverAddr: serverAddr,
		context:    ctx,
		cancel:     cancel,
		options: &options{
			asyncProducerOpts: asyncProducerOptions{
				batch: struct {
					batchSize int
					linger    time.Duration
				}{
					batchSize: defaultBatchSize,
					linger:    time.Duration(0),
				},
			},
			consumerOpts: consumerOptions{
				offsetsAutoCommit: struct {
					enabled  bool
					interval time.Duration
				}{
					enabled:  true,
					interval: 1 * time.Second,
				},
			},
		},
	}

	for _, opt := range opts {
		if err := opt(cli); err != nil {
			return nil, err
		}
	}

	if cli.lg == nil {
		cli.lg = zap.NewNop()
	}

	cli.addDialOpts()
	cli.addCallOpts()

	conn, err := cli.dial()
	if err != nil {
		cli.cancel()
		return nil, err
	}

	cli.lg.Info("Start background dial", zap.String("target", cli.serverAddr))

	cli.conn = conn
	// client.Auth = NewAuth(client) // TODO

	return cli, nil
}
