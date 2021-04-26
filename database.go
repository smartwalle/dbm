package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

type Database interface {
	Client() *Client

	Database() *mongo.Database

	Name() string

	Drop(ctx context.Context) error

	Collection(name string) Collection

	WithTransaction(ctx context.Context, fn func(sCtx context.Context) (interface{}, error), opts ...*TransactionOptions) (interface{}, error)

	UseSession(ctx context.Context, fn func(sess Session) error) error

	StartSession(ctx context.Context) (Session, error)
}

type database struct {
	database *mongo.Database
	client   *Client
}

func (this *database) Client() *Client {
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

func (this *database) Collection(name string) Collection {
	return &collection{collection: this.database.Collection(name), database: this}
}

func (this *database) WithTransaction(ctx context.Context, fn func(sCtx context.Context) (interface{}, error), opts ...*TransactionOptions) (interface{}, error) {
	return this.client.WithTransaction(ctx, fn, opts...)
}

func (this *database) UseSession(ctx context.Context, fn func(sess Session) error) error {
	return this.client.UseSession(ctx, fn)
}

func (this *database) StartSession(ctx context.Context) (Session, error) {
	return this.client.StartSession(ctx)
}
