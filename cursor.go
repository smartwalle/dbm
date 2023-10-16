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

func (c *cursor) ID() int64 {
	if c.err != nil {
		return 0
	}
	return c.Cursor.ID()
}

func (c *cursor) Next(ctx context.Context) bool {
	if c.err != nil {
		return false
	}
	return c.Cursor.Next(ctx)
}

func (c *cursor) TryNext(ctx context.Context) bool {
	if c.err != nil {
		return false
	}
	return c.Cursor.TryNext(ctx)
}

func (c *cursor) One(result interface{}) error {
	if c.err != nil {
		return c.err
	}
	return c.Cursor.Decode(result)
}

func (c *cursor) All(ctx context.Context, result interface{}) error {
	if c.err != nil {
		return c.err
	}
	return c.Cursor.All(ctx, result)
}

func (c *cursor) RemainingBatchLength() int {
	if c.err != nil {
		return 0
	}
	return c.Cursor.RemainingBatchLength()
}

func (c *cursor) Close(ctx context.Context) error {
	if c.err != nil {
		return c.err
	}
	return c.Cursor.Close(ctx)
}

func (c *cursor) Error() error {
	if c.err != nil {
		return c.err
	}
	return c.Cursor.Err()
}
