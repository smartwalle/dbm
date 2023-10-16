package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type aggregator interface {
	Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (*mongo.Cursor, error)
}

type Aggregate interface {
	AllowDiskUse(b bool) Aggregate

	BatchSize(n int32) Aggregate

	BypassDocumentValidation(b bool) Aggregate

	Collation(c *Collation) Aggregate

	Comment(s string) Aggregate

	Hint(hint interface{}) Aggregate

	MaxTime(d time.Duration) Aggregate

	MaxAwaitTime(d time.Duration) Aggregate

	One(result interface{}) error

	All(result interface{}) error

	Cursor() Cursor
}

type aggregate struct {
	pipeline interface{}

	ctx        context.Context
	opts       *options.AggregateOptions
	aggregator aggregator
}

func (ag *aggregate) AllowDiskUse(b bool) Aggregate {
	ag.opts.SetAllowDiskUse(b)
	return ag
}

func (ag *aggregate) BatchSize(n int32) Aggregate {
	ag.opts.SetBatchSize(n)
	return ag
}

func (ag *aggregate) BypassDocumentValidation(b bool) Aggregate {
	ag.opts.SetBypassDocumentValidation(b)
	return ag
}

func (ag *aggregate) Collation(c *Collation) Aggregate {
	ag.opts.SetCollation(c)
	return ag
}

func (ag *aggregate) Comment(s string) Aggregate {
	ag.opts.SetComment(s)
	return ag
}

func (ag *aggregate) Hint(hint interface{}) Aggregate {
	ag.opts.SetHint(hint)
	return ag
}

func (ag *aggregate) MaxTime(d time.Duration) Aggregate {
	ag.opts.SetMaxTime(d)
	return ag
}

func (ag *aggregate) MaxAwaitTime(d time.Duration) Aggregate {
	ag.opts.SetMaxAwaitTime(d)
	return ag
}

func (ag *aggregate) One(result interface{}) error {
	var cur = ag.Cursor()
	defer cur.Close(ag.ctx)
	if cur.Next(ag.ctx) {
		return cur.One(result)
	}
	return cur.Error()
}

func (ag *aggregate) All(result interface{}) error {
	var cur = ag.Cursor()
	defer cur.Close(ag.ctx)
	return cur.All(ag.ctx, result)
}

func (ag *aggregate) Cursor() Cursor {
	var cur, err = ag.aggregator.Aggregate(ag.ctx, ag.pipeline, ag.opts)
	return &cursor{Cursor: cur, err: err}
}
