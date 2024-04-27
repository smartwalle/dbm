package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"math"
	"strconv"
	"strings"
	"time"
)

type Client interface {
	Client() *mongo.Client

	Registry() *bsoncodec.Registry

	Close(ctx context.Context) error

	Ping(ctx context.Context) error

	ServerStatus(ctx context.Context) (bson.Raw, error)

	ServerVersion() string

	TransactionAllowed() bool

	Database(name string, opts ...*DatabaseOptions) Database

	UseSession(ctx context.Context, fn func(SessionContext) error) error

	UseSessionWithOptions(ctx context.Context, opts *options.SessionOptions, fn func(SessionContext) error) error

	StartSession(opts ...*SessionOptions) (Session, error)

	Begin(ctx context.Context, opts ...*TransactionOptions) (Tx, error)

	Watch(ctx context.Context, pipeline interface{}) Watcher
}

type client struct {
	*serverInfo
	config   *Config
	topology *topology.Topology
	client   *mongo.Client
}

type serverInfo struct {
	version            string
	transactionAllowed bool
}

func New(ctx context.Context, cfg *Config) (Client, error) {
	var nTopology, err = connectTopology(cfg.ClientOptions)
	if err != nil {
		return nil, err
	}

	if cfg.ClientOptions.Registry == nil {
		cfg.ClientOptions.SetRegistry(bson.DefaultRegistry)
	}

	mClient, err := connect(ctx, cfg.ClientOptions)
	if err != nil {
		return nil, err
	}

	sInfo, err := loadServerInfo(ctx, nTopology, mClient)
	if err != nil {
		return nil, err
	}

	var nClient = &client{}
	nClient.serverInfo = sInfo
	nClient.config = cfg
	nClient.topology = nTopology
	nClient.client = mClient
	return nClient, nil
}

func connectTopology(opts *options.ClientOptions) (*topology.Topology, error) {
	cfg, err := topology.NewConfig(opts, nil)
	if err != nil {
		return nil, err
	}

	nTopology, err := topology.New(cfg)
	if err != nil {
		return nil, err
	}

	if err = nTopology.Connect(); err != nil {
		return nil, err
	}
	return nTopology, nil
}

func connect(ctx context.Context, opts *options.ClientOptions) (*mongo.Client, error) {
	var nClient, err = mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	var nCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err = nClient.Ping(nCtx, readpref.Primary()); err != nil {
		return nil, err
	}
	return nClient, nil
}

func loadServerInfo(ctx context.Context, topo *topology.Topology, client *mongo.Client) (*serverInfo, error) {
	// 获取服务器状态
	status, err := serverStatus(ctx, client)
	if err != nil {
		return nil, err
	}

	// 获取版本信息
	value, err := status.LookupErr("version")
	if err != nil {
		return nil, err
	}
	var version = value.StringValue()

	var info = &serverInfo{}
	info.version = version
	info.transactionAllowed = topo.Kind() != description.Single && CompareServerVersions(info.version, "4.0.0") > 0
	return info, nil
}

func serverStatus(ctx context.Context, client *mongo.Client) (bson.Raw, error) {
	var status bson.Raw
	if err := client.Database("admin").RunCommand(ctx, bson.D{{"serverStatus", 1}}).Decode(&status); err != nil {
		return nil, err
	}
	return status, nil
}

func (c *client) Client() *mongo.Client {
	return c.client
}

func (c *client) Registry() *bsoncodec.Registry {
	return c.config.Registry
}

func (c *client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

func (c *client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, readpref.Primary())
}

func (c *client) ServerStatus(ctx context.Context) (bson.Raw, error) {
	return serverStatus(ctx, c.client)
}

func (c *client) ServerVersion() string {
	return c.version
}

func (c *client) TransactionAllowed() bool {
	return c.transactionAllowed
}

func (c *client) Database(name string, opts ...*DatabaseOptions) Database {
	return &database{database: c.client.Database(name, opts...), client: c}
}

func (c *client) UseSession(ctx context.Context, fn func(SessionContext) error) error {
	if !c.transactionAllowed {
		return ErrSessionNotSupported
	}
	return c.client.UseSession(ctx, fn)
}

func (c *client) UseSessionWithOptions(ctx context.Context, opts *options.SessionOptions, fn func(SessionContext) error) error {
	if !c.transactionAllowed {
		return ErrSessionNotSupported
	}
	return c.client.UseSessionWithOptions(ctx, opts, fn)
}

func (c *client) startSession(opts ...*SessionOptions) (mongo.Session, error) {
	if !c.transactionAllowed {
		return nil, ErrSessionNotSupported
	}
	return c.client.StartSession(opts...)
}

func (c *client) StartSession(opts ...*SessionOptions) (Session, error) {
	var sess, err = c.startSession(opts...)
	if err != nil {
		return nil, err
	}
	return &session{sess}, nil
}

func (c *client) Begin(ctx context.Context, opts ...*TransactionOptions) (Tx, error) {
	var sess, err = c.startSession()
	if err != nil {
		return nil, err
	}

	if err = sess.StartTransaction(opts...); err != nil {
		sess.EndSession(ctx)
		return nil, err
	}
	return &transaction{mongo.NewSessionContext(ctx, sess), true}, nil
}

func (c *client) Watch(ctx context.Context, pipeline interface{}) Watcher {
	var w = &watch{}
	w.pipeline = pipeline
	w.ctx = ctx
	w.opts = options.ChangeStream()
	w.watcher = c.client
	return w
}

func CompareServerVersions(v1 string, v2 string) int {
	n1 := strings.Split(v1, ".")
	n2 := strings.Split(v2, ".")

	for i := 0; i < int(math.Min(float64(len(n1)), float64(len(n2)))); i++ {
		i1, err := strconv.Atoi(n1[i])
		if err != nil {
			return 1
		}

		i2, err := strconv.Atoi(n2[i])
		if err != nil {
			return -1
		}

		difference := i1 - i2
		if difference != 0 {
			return difference
		}
	}
	return 0
}
