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

	UseSessionWithOptions(ctx context.Context, opts *SessionOptions, fn func(sess Session) error) error

	StartSession(ctx context.Context, opts ...*SessionOptions) (Session, error)

	Watch(ctx context.Context, pipeline interface{}) Watcher
}

type database struct {
	database *mongo.Database
	client   Client
}

func (db *database) Client() Client {
	return db.client
}

func (db *database) Database() *mongo.Database {
	return db.database
}

func (db *database) Name() string {
	return db.database.Name()
}

func (db *database) Drop(ctx context.Context) error {
	return db.database.Drop(ctx)
}

func (db *database) Collection(name string, opts ...*CollectionOptions) Collection {
	return &collection{collection: db.database.Collection(name, opts...), database: db}
}

func (db *database) WithTransaction(ctx context.Context, fn func(sCtx SessionContext) (interface{}, error), opts ...*TransactionOptions) (interface{}, error) {
	return db.client.WithTransaction(ctx, fn, opts...)
}

func (db *database) UseSession(ctx context.Context, fn func(sess Session) error) error {
	return db.client.UseSession(ctx, fn)
}

func (db *database) UseSessionWithOptions(ctx context.Context, opts *SessionOptions, fn func(sess Session) error) error {
	return db.client.UseSessionWithOptions(ctx, opts, fn)
}

func (db *database) StartSession(ctx context.Context, opts ...*SessionOptions) (Session, error) {
	return db.client.StartSession(ctx, opts...)
}

func (db *database) Aggregate(ctx context.Context, pipeline interface{}) Aggregate {
	var a = &aggregate{}
	a.pipeline = pipeline
	a.ctx = ctx
	a.opts = options.Aggregate()
	a.aggregator = db.database
	return a
}

func (db *database) Watch(ctx context.Context, pipeline interface{}) Watcher {
	var w = &watch{}
	w.pipeline = pipeline
	w.ctx = ctx
	w.opts = options.ChangeStream()
	w.watcher = db.database
	return w
}
