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

type SessionContext = mongo.SessionContext

type Session interface {
	mongo.Session

	BeginTx(ctx context.Context, opts ...*TransactionOptions) (Tx, error)
}

type session struct {
	mongo.Session
}

func (s *session) BeginTx(ctx context.Context, opts ...*TransactionOptions) (Tx, error) {
	if err := s.Session.StartTransaction(opts...); err != nil {
		return nil, err
	}
	return &transaction{mongo.NewSessionContext(ctx, s.Session), false}, nil
}
