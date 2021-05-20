package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

type Cursor interface {
	ID() int64

	Next(ctx context.Context) bool

	TryNext(ctx context.Context) bool

	One(result interface{}) error

	All(ctx context.Context, result interface{}) error

	RemainingBatchLength() int

	Close(ctx context.Context) error

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

func (this *cursor) One(result interface{}) error {
	if this.err != nil {
		return this.err
	}
	return this.Cursor.Decode(result)
}

func (this *cursor) All(ctx context.Context, result interface{}) error {
	if this.err != nil {
		return this.err
	}
	return this.Cursor.All(ctx, result)
}

func (this *cursor) RemainingBatchLength() int {
	if this.err != nil {
		return 0
	}
	return this.Cursor.RemainingBatchLength()
}

func (this *cursor) Close(ctx context.Context) error {
	if this.err != nil {
		return this.err
	}
	return this.Cursor.Close(ctx)
}

func (this *cursor) Error() error {
	if this.err != nil {
		return this.err
	}
	return this.Cursor.Err()
}
