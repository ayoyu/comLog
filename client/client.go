package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type Option func(*Client) error

type Client struct {
	ComLogClient

	*options
	serverAddr  string
	context     context.Context
	cancel      context.CancelFunc
	dialTimeout time.Duration
	dialOpts    []grpc.DialOption
	callOpts    []grpc.CallOption
	// The ClientConn contains one or more actual connections to real server backends and attempts
	// to keep these connections healthy by automatically reconnecting to them when they break.
	// Ref: https://github.com/grpc/grpc-go/blob/master/Documentation/anti-patterns.md
	conn *grpc.ClientConn
}

// WithRetryPolicyOption configures the retry policy option.
// Ref: https://github.com/grpc/grpc/blob/master/doc/service_config.md
func WithRetryPolicy(opt RetryOptionParameters) Option {
	return func(c *Client) error {
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
		c.serviceCfgRawJSON = string(b)
		return nil
	}
}

// TODO
func WithAuth(username, password string) Option {
	return func(c *Client) error {
		c.username = username
		c.password = password
		return nil
	}
}

// WithTLS configures the client-side tls configuration with the given root certificate authority certificate file
// to validate the server connection. The serverNameOverride is only for testing, if not empty it will override
// the virtual host name of authority in requests.
func WithTLS(rootCAFile, serverNameOverride string) Option {
	return func(c *Client) error {
		creds, err := credentials.NewClientTLSFromFile(rootCAFile, serverNameOverride)
		if err != nil {
			return err
		}
		c.creds = creds
		return nil
	}
}

// WithDialTimeout configures the timeout for failing to establish a dial connection with the server.
// This is valid if and only if WithBlock() is present, if dial is non-blocking the timeout will be ignored.
func WithDialTimeout(timeout time.Duration) Option {
	return func(c *Client) error {
		c.dialTimeout = timeout
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
	return func(c *Client) error {
		c.dialBlock = true
		return nil
	}
}

// WithKeepAliveProbe configures the keep-alive-probe to ping the server for liveness.
// `dialTime` is the time after which client pings the server to see if transport is alive with `dialTimeout`
// which is the time that the client waits for a response for the keep-alive probe, if the response is not received
// in this time, the connection is closed. When `permitWithoutStream` is set to true the client will send keepalive pings
// to the server without any active streams(RPCs).
func WithKeepAliveProbe(dialTime, dialTimeout time.Duration, permitWithoutStream bool) Option {
	return func(c *Client) error {
		c.dialKeepAliveTime = dialTime
		c.dialKeepAliveTimeout = dialTimeout
		c.permitWithoutStream = permitWithoutStream
		return nil
	}
}

// WithMaxCallSendRecvMsgSize configures the client-side request/response send/receive limit sizein bytes.
// The default values for `sendBytes` and `recvBytes` if they are not set are 2 MB (= 2 * 1024 * 1024) and
// `math.MaxInt32` respectively.
// Make sure that "client-side send limit < server-side default send/recv limit" and
// "client-side receive limit >= server-side default send/recv limit" because range response
// can easily exceed request send limits
func WithMaxSendRecvMsgSize(sendBytes, recvBytes int) Option {
	return func(c *Client) error {
		if recvBytes > 0 {
			if sendBytes == 0 && recvBytes < defaultmaxSendMsgSize {
				return fmt.Errorf(
					"If the send limit (%d bytes) is not defined, the recv limit (%d bytes) must be greater than 2MB (default)",
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
		c.maxCallSendMsgSize = sendBytes
		c.maxCallRecvMsgSize = recvBytes
		return nil
	}
}

func (c *Client) addDialOpts() {
	if c.serviceCfgRawJSON != "" {
		c.dialOpts = append(c.dialOpts, grpc.WithDefaultServiceConfig(c.serviceCfgRawJSON))
	} else {
		c.dialOpts = append(c.dialOpts, defaultServiceConfig)
	}

	if c.creds != nil {
		c.dialOpts = append(c.dialOpts, grpc.WithTransportCredentials(c.creds))
	} else {
		c.dialOpts = append(c.dialOpts, defaultTLSInsecureCreds)
	}

	if c.dialKeepAliveTime > 0 {
		// TODO: define default probe values ?
		params := keepalive.ClientParameters{
			Time:                c.dialKeepAliveTime,
			Timeout:             c.dialKeepAliveTimeout,
			PermitWithoutStream: c.permitWithoutStream,
		}
		c.dialOpts = append(c.dialOpts, grpc.WithKeepaliveParams(params))
	}

	if c.dialBlock {
		c.dialOpts = append(c.dialOpts, grpc.WithBlock())
	}
}

func (c *Client) addCallOpts() {
	c.callOpts = append(c.callOpts, defaultWaitForReady)

	if c.maxCallSendMsgSize > 0 {
		c.callOpts = append(c.callOpts, grpc.MaxCallSendMsgSize(c.maxCallSendMsgSize))
	} else {
		c.callOpts = append(c.callOpts, defaultMaxCallSendMsgSize)
	}

	if c.maxCallRecvMsgSize > 0 {
		c.callOpts = append(c.callOpts, grpc.MaxCallRecvMsgSize(c.maxCallRecvMsgSize))
	} else {
		c.callOpts = append(c.callOpts, defaultMaxCallRecvMsgSize)
	}
}

func (c *Client) Dial() (*grpc.ClientConn, error) {
	dctx := c.context
	if c.dialTimeout > 0 {
		var cancel context.CancelFunc
		dctx, cancel = context.WithTimeout(c.context, c.dialTimeout)
		defer cancel()
	}

	return grpc.DialContext(dctx, c.serverAddr, c.dialOpts...)
}

// Creates new commit log gRPC client with the given server address and options. The context is the default
// client context, it can be used to cancel grpc dial out and other operations that do not have an explicit context.
func New(ctx context.Context, serverAddr string, opts ...Option) (*Client, error) {
	baseCtx := context.TODO()
	if ctx != nil {
		baseCtx = ctx
	}
	ctx, cancel := context.WithCancel(baseCtx)

	cli := &Client{
		serverAddr: serverAddr,
		context:    ctx,
		cancel:     cancel,
		options:    new(options),
	}

	for _, opt := range opts {
		if err := opt(cli); err != nil {
			return nil, err
		}
	}
	cli.addDialOpts()
	cli.addCallOpts()

	conn, err := cli.Dial()
	if err != nil {
		cli.cancel()
		return nil, err
	}
	logrus.Infof("Start background dial with target: %s", cli.serverAddr)
	cli.conn = conn

	cli.ComLogClient = NewClientComLog(cli)
	// client.Auth = NewAuth(client) // TODO

	return cli, nil
}
