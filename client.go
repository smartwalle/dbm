package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	*info
	cfg    *Config
	client *mongo.Client
}

type info struct {
	version            string
	transactionAllowed bool
}

func NewClient(ctx context.Context, cfg *Config) (*Client, error) {
	var client, err = connect(ctx, cfg.ClientOptions)
	if err != nil {
		return nil, err
	}

	sInfo, err := load(ctx, client)
	if err != nil {
		return nil, err
	}

	var nClient = &Client{}
	nClient.info = sInfo
	nClient.cfg = cfg
	nClient.client = client
	return nClient, nil
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

func load(ctx context.Context, client *mongo.Client) (*info, error) {
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

	var versions = strings.Split(version, ".")
	if len(versions) > 0 {
		if val, _ := strconv.Atoi(versions[0]); val >= 4 {
			sInfo.transactionAllowed = true
		}
	}
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

func (this *Client) Close() error {
	return this.client.Disconnect(context.Background())
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

func (this *Client) Database(name string) *Database {
	return &Database{Database: this.client.Database(name), client: this}
}
