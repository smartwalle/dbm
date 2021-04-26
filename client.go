package dbm

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"go.mongodb.org/mongo-driver/x/mongo/driver/ocsp"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"
	"math"
	"strconv"
	"strings"
	"time"
)

var (
	ErrSessionNotSupported = errors.New("session not supported")
)

type Client struct {
	*info
	cfg    *Config
	topo   *topology.Topology
	client *mongo.Client
}

type info struct {
	version            string
	transactionAllowed bool
}

func NewClient(ctx context.Context, cfg *Config) (*Client, error) {
	var topo, err = connectTopology(cfg)
	if err != nil {
		return nil, err
	}

	client, err := connect(ctx, cfg.ClientOptions)
	if err != nil {
		return nil, err
	}

	sInfo, err := load(ctx, topo, client)
	if err != nil {
		return nil, err
	}

	var nClient = &Client{}
	nClient.info = sInfo
	nClient.cfg = cfg
	nClient.topo = topo
	nClient.client = client
	return nClient, nil
}

func connectTopology(cfg *Config) (*topology.Topology, error) {
	connectionOpts := []topology.ConnectionOption{
		topology.WithOCSPCache(func(ocsp.Cache) ocsp.Cache {
			return ocsp.NewCache()
		}),
	}
	serverOpts := []topology.ServerOption{
		topology.WithConnectionOptions(func(opts ...topology.ConnectionOption) []topology.ConnectionOption {
			return append(opts, connectionOpts...)
		}),
	}

	var topo, err = topology.New(topology.WithConnString(func(connString connstring.ConnString) connstring.ConnString {
		var connStr, _ = connstring.ParseAndValidate(cfg.GetURI())
		return connStr
	}), topology.WithServerOptions(func(option ...topology.ServerOption) []topology.ServerOption {
		return append(option, serverOpts...)
	}))

	if err != nil {
		return nil, err
	}

	if err = topo.Connect(); err != nil {
		return nil, err
	}
	return topo, nil
}

func connect(ctx context.Context, opts *options.ClientOptions) (*mongo.Client, error) {
	var client, err = mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	var nCtx, cancel = context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err = client.Ping(nCtx, readpref.Primary()); err != nil {
		return nil, err
	}
	return client, nil
}

func load(ctx context.Context, topo *topology.Topology, client *mongo.Client) (*info, error) {
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

	var sInfo = &info{}
	sInfo.version = version
	sInfo.transactionAllowed = topo.Kind() != description.Single && CompareServerVersions(sInfo.version, "4.0.0") > 0
	return sInfo, nil
}

func serverStatus(ctx context.Context, client *mongo.Client) (bson.Raw, error) {
	var status bson.Raw
	if err := client.Database("admin").RunCommand(ctx, bson.D{{"serverStatus", 1}}).Decode(&status); err != nil {
		return nil, err
	}
	return status, nil
}

func (this *Client) Client() *mongo.Client {
	return this.client
}

func (this *Client) Close(ctx context.Context) error {
	return this.client.Disconnect(ctx)
}

func (this *Client) Ping(ctx context.Context) error {
	return this.client.Ping(ctx, readpref.Primary())
}

func (this *Client) ServerStatus(ctx context.Context) (bson.Raw, error) {
	return serverStatus(ctx, this.client)
}

func (this *Client) ServerVersion() string {
	return this.version
}

func (this *Client) TransactionAllowed() bool {
	return this.transactionAllowed
}

func (this *Client) Database(name string) *database {
	return &database{database: this.client.Database(name), client: this}
}

// WithTransaction
// var client, _ = dbm.NewClient(...)
// var db = client.Database("xx")
// var c1 = db.Collection("c1")
// var c2 = db.Collection("c2")
// db.WithTransaction(context.Background(), func(sCtx context.Context) (interface{}, error) {
//		if _, sErr := c1.Insert(sCtx, ...); sErr != nil {
//			return nil, sErr
//		}
//		if _, sErr := c2.Insert(sCtx, ...); sErr != nil {
//			return nil, sErr
//		}
//		return nil, nil
// }
func (this *Client) WithTransaction(ctx context.Context, fn func(sCtx context.Context) (interface{}, error), opts ...*TransactionOptions) (interface{}, error) {
	var sess, err = this.StartSession(ctx)
	if err != nil {
		return nil, err
	}
	defer sess.EndSession(ctx)
	return sess.WithTransaction(ctx, fn, opts...)
}

// UseSession
// var client, _ = dbm.NewClient(...)
// var db = client.Database("xx")
// var c1 = db.Collection("c1")
// var c2 = db.Collection("c2")
// db.UseSession(context.Background(), func(sess dbm.Session) error {
// 		if sErr := sess.StartTransaction(); sErr != nil {
//			return sErr
//		}
//		if _, sErr := c1.Insert(sess, ...); sErr != nil {
//			return nil, sErr
//		}
//		if _, sErr := c2.Insert(sess, ...); sErr != nil {
//			return nil, sErr
//		}
// 		return sess.CommitTransaction(context.Background())
// })
func (this *Client) UseSession(ctx context.Context, fn func(sess Session) error) error {
	if this.transactionAllowed == false {
		return ErrSessionNotSupported
	}
	return this.client.UseSession(ctx, func(sCtx SessionContext) error {
		var s = &session{}
		s.SessionContext = sCtx
		return fn(s)
	})
}

// StartSession
// var client, _ = dbm.NewClient(...)
// var db = client.Database("xx")
// var c1 = db.Collection("c1")
// var c2 = db.Collection("c2")
// var sess, _ = db.StartSession(context.Background())
// defer sess.EndSession(context.Background())
// if sErr := sess.StartTransaction(); sErr != nil {
// 		return
// }
// if _, sErr := c1.Insert(sess, ...); sErr != nil {
// 		return sErr
// }
// if _, sErr := c2.Insert(sess, ...); sErr != nil {
// 		return sErr
// }
// sess.CommitTransaction(context.Background())
func (this *Client) StartSession(ctx context.Context) (Session, error) {
	if this.transactionAllowed == false {
		return nil, ErrSessionNotSupported
	}

	var sess, err = this.client.StartSession()
	if err != nil {
		return nil, err
	}
	return &session{SessionContext: mongo.NewSessionContext(ctx, sess)}, nil
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
