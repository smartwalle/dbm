package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SessionContext = mongo.SessionContext

type TransactionOptions = options.TransactionOptions

func Transaction() *TransactionOptions {
	return options.Transaction()
}

type Session interface {
	context.Context

	Context() SessionContext

	StartTransaction(...*TransactionOptions) error

	AbortTransaction(context.Context) error

	CommitTransaction(context.Context) error

	WithTransaction(context.Context, func(ctx context.Context) (interface{}, error), ...*TransactionOptions) (interface{}, error)

	EndSession(context.Context)

	//ClusterTime() bson.Raw
	//
	//OperationTime() *primitive.Timestamp
	//
	//Client() *Client
	//
	//ID() bson.Raw
	//
	//AdvanceClusterTime(bson.Raw) error
	//
	//AdvanceOperationTime(*primitive.Timestamp) error
}

type session struct {
	SessionContext
}

func (this *session) Context() SessionContext {
	return this.SessionContext
}

func (this *session) StartTransaction(opts ...*TransactionOptions) error {
	return this.SessionContext.StartTransaction(opts...)
}

func (this *session) AbortTransaction(ctx context.Context) error {
	return this.SessionContext.AbortTransaction(ctx)
}

func (this *session) CommitTransaction(ctx context.Context) error {
	return this.SessionContext.CommitTransaction(ctx)
}

func (this *session) WithTransaction(ctx context.Context, fn func(ctx context.Context) (interface{}, error), opts ...*TransactionOptions) (interface{}, error) {
	return this.SessionContext.WithTransaction(ctx, func(sCtx mongo.SessionContext) (interface{}, error) {
		return fn(this.SessionContext)
	}, opts...)
}

func (this *session) EndSession(ctx context.Context) {
	this.SessionContext.EndSession(ctx)
}
