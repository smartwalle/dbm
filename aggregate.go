package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

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
	collection *mongo.Collection
}

func (this *aggregate) AllowDiskUse(b bool) Aggregate {
	this.opts.SetAllowDiskUse(b)
	return this
}

func (this *aggregate) BatchSize(n int32) Aggregate {
	this.opts.SetBatchSize(n)
	return this
}

func (this *aggregate) BypassDocumentValidation(b bool) Aggregate {
	this.opts.SetBypassDocumentValidation(b)
	return this
}

func (this *aggregate) Collation(c *Collation) Aggregate {
	this.opts.SetCollation(c)
	return this
}

func (this *aggregate) Comment(s string) Aggregate {
	this.opts.SetComment(s)
	return this
}

func (this *aggregate) Hint(hint interface{}) Aggregate {
	this.opts.SetHint(hint)
	return this
}

func (this *aggregate) MaxTime(d time.Duration) Aggregate {
	this.opts.SetMaxTime(d)
	return this
}

func (this *aggregate) MaxAwaitTime(d time.Duration) Aggregate {
	this.opts.SetMaxAwaitTime(d)
	return this
}

func (this *aggregate) One(result interface{}) error {
	var cur = this.Cursor()
	defer cur.Close(this.ctx)
	if cur.Next(this.ctx) {
		return cur.One(this.ctx, result)
	}
	return cur.Error()
}

func (this *aggregate) All(result interface{}) error {
	var cur = this.Cursor()
	defer cur.Close(this.ctx)
	return cur.All(this.ctx, result)
}

func (this *aggregate) Cursor() Cursor {
	var cur, err = this.collection.Aggregate(this.ctx, this.pipeline, this.opts)
	return &cursor{Cursor: cur, err: err}
}
