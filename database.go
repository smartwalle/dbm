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

	UseSession(ctx context.Context, fn func(SessionContext) error) error

	UseSessionWithOptions(ctx context.Context, opts *options.SessionOptions, fn func(SessionContext) error) error

	StartSession(opts ...*SessionOptions) (Session, error)

	Begin(ctx context.Context, opts ...*TransactionOptions) (Tx, error)

	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*ChangeStream, error)
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

func (db *database) Aggregate(ctx context.Context, pipeline interface{}) Aggregate {
	var a = &aggregate{}
	a.pipeline = pipeline
	a.ctx = ctx
	a.opts = options.Aggregate()
	a.aggregator = db.database
	return a
}

func (db *database) UseSession(ctx context.Context, fn func(SessionContext) error) error {
	return db.client.UseSession(ctx, fn)
}

func (db *database) UseSessionWithOptions(ctx context.Context, opts *options.SessionOptions, fn func(SessionContext) error) error {
	return db.client.UseSessionWithOptions(ctx, opts, fn)
}

func (db *database) StartSession(opts ...*SessionOptions) (Session, error) {
	return db.client.StartSession(opts...)
}

func (db *database) Begin(ctx context.Context, opts ...*TransactionOptions) (Tx, error) {
	return db.client.Begin(ctx, opts...)
}

func (db *database) Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*ChangeStream, error) {
	return db.database.Watch(ctx, pipeline, opts...)
}
