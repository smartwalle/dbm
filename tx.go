package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

type Tx interface {
	context.Context

	Commit(ctx context.Context) error

	Rollback(ctx context.Context) error
}

type transaction struct {
	mongo.SessionContext
	automatic bool
}

func (tx *transaction) Commit(ctx context.Context) error {
	if tx.automatic {
		defer tx.SessionContext.EndSession(ctx)
	}
	return tx.SessionContext.CommitTransaction(ctx)
}

func (tx *transaction) Rollback(ctx context.Context) error {
	if tx.automatic {
		defer tx.SessionContext.EndSession(ctx)
	}
	return tx.SessionContext.AbortTransaction(ctx)
}
