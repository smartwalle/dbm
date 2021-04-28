package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TransactionOptions = options.TransactionOptions

func Transaction() *TransactionOptions {
	return options.Transaction()
}

type SessionContext interface {
	context.Context

	AbortTransaction(context.Context) error

	CommitTransaction(context.Context) error
}

type Session interface {
	SessionContext

	Context() SessionContext

	StartTransaction(...*TransactionOptions) error

	WithTransaction(context.Context, func(SessionContext) (interface{}, error), ...*TransactionOptions) (interface{}, error)

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
	mongo.SessionContext
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

func (this *session) WithTransaction(ctx context.Context, fn func(SessionContext) (interface{}, error), opts ...*TransactionOptions) (interface{}, error) {
	return this.SessionContext.WithTransaction(ctx, func(sCtx mongo.SessionContext) (interface{}, error) {
		return fn(this)
	}, opts...)
}

func (this *session) EndSession(ctx context.Context) {
	this.SessionContext.EndSession(ctx)
}
