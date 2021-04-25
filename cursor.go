package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

type Cursor interface {
	ID() int64

	Next(ctx context.Context) bool

	TryNext(ctx context.Context) bool

	One(ctx context.Context, results interface{}) error

	All(ctx context.Context, results interface{}) error

	RemainingBatchLength() int

	Close() error

	Error() error
}

type cursor struct {
	*mongo.Cursor
	err error
}

func (this *cursor) ID() int64 {
	if this.err != nil {
		return 0
	}
	return this.Cursor.ID()
}

func (this *cursor) Next(ctx context.Context) bool {
	if this.err != nil {
		return false
	}
	return this.Cursor.Next(ctx)
}

func (this *cursor) TryNext(ctx context.Context) bool {
	if this.err != nil {
		return false
	}
	return this.Cursor.TryNext(ctx)
}

func (this *cursor) One(ctx context.Context, results interface{}) error {
	if this.err != nil {
		return this.err
	}
	return this.Cursor.Decode(results)
}

func (this *cursor) All(ctx context.Context, results interface{}) error {
	if this.err != nil {
		return this.err
	}
	return this.Cursor.All(ctx, results)
}

func (this *cursor) RemainingBatchLength() int {
	if this.err != nil {
		return 0
	}
	return this.Cursor.RemainingBatchLength()
}

func (this *cursor) Close() error {
	if this.err != nil {
		return this.err
	}
	return this.Cursor.Close(context.Background())
}

func (this *cursor) Error() error {
	if this.err != nil {
		return this.err
	}
	return this.Cursor.Err()
}
