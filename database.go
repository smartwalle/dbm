package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DatabaseOptions = options.DatabaseOptions

func NewDatabaseOptions() *DatabaseOptions {
	return options.Database()
}

type Database interface {
	Client() Client

	Database() *mongo.Database

	Name() string

	Drop(ctx context.Context) error

	Collection(name string, opts ...*CollectionOptions) Collection

	WithTransaction(ctx context.Context, fn func(sCtx SessionContext) (interface{}, error), opts ...*TransactionOptions) (interface{}, error)

	UseSession(ctx context.Context, fn func(sess Session) error) error

	StartSession(ctx context.Context) (Session, error)

	Watch(ctx context.Context, pipeline interface{}) Watcher
}

type database struct {
	database *mongo.Database
	client   Client
}

func (this *database) Client() Client {
	return this.client
}

func (this *database) Database() *mongo.Database {
	return this.database
}

func (this *database) Name() string {
	return this.database.Name()
}

func (this *database) Drop(ctx context.Context) error {
	return this.database.Drop(ctx)
}

func (this *database) Collection(name string, opts ...*CollectionOptions) Collection {
	return &collection{collection: this.database.Collection(name, opts...), database: this}
}

func (this *database) WithTransaction(ctx context.Context, fn func(sCtx SessionContext) (interface{}, error), opts ...*TransactionOptions) (interface{}, error) {
	return this.client.WithTransaction(ctx, fn, opts...)
}

func (this *database) UseSession(ctx context.Context, fn func(sess Session) error) error {
	return this.client.UseSession(ctx, fn)
}

func (this *database) StartSession(ctx context.Context) (Session, error) {
	return this.client.StartSession(ctx)
}

func (this *database) Aggregate(ctx context.Context, pipeline interface{}) Aggregate {
	var a = &aggregate{}
	a.pipeline = pipeline
	a.ctx = ctx
	a.opts = options.Aggregate()
	a.aggregator = this.database
	return a
}

func (this *database) Watch(ctx context.Context, pipeline interface{}) Watcher {
	var w = &watch{}
	w.pipeline = pipeline
	w.ctx = ctx
	w.opts = options.ChangeStream()
	w.watcher = this.database
	return w
}
