package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SessionOptions = options.SessionOptions

func NewSessionOptions() *SessionOptions {
	return options.Session()
}

type TransactionOptions = options.TransactionOptions

func NewTransactionOptions() *TransactionOptions {
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

	WithTransaction(context.Context, func(sCtx SessionContext) (interface{}, error), ...*TransactionOptions) (interface{}, error)

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

func (s *session) Context() SessionContext {
	return s.SessionContext
}

func (s *session) StartTransaction(opts ...*TransactionOptions) error {
	return s.SessionContext.StartTransaction(opts...)
}

func (s *session) AbortTransaction(ctx context.Context) error {
	return s.SessionContext.AbortTransaction(ctx)
}

func (s *session) CommitTransaction(ctx context.Context) error {
	return s.SessionContext.CommitTransaction(ctx)
}

func (s *session) WithTransaction(ctx context.Context, fn func(sCtx SessionContext) (interface{}, error), opts ...*TransactionOptions) (interface{}, error) {
	return s.SessionContext.WithTransaction(ctx, func(sessionCtx mongo.SessionContext) (interface{}, error) {
		return fn(sessionCtx)
	}, opts...)
}

func (s *session) EndSession(ctx context.Context) {
	s.SessionContext.EndSession(ctx)
}
