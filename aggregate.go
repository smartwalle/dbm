package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type Aggregate interface {
	Hint(hint interface{}) Aggregate
	BatchSize(n int32) Aggregate
	AllowDiskUse(b bool) Aggregate
	MaxTime(time.Duration) Aggregate

	One(result interface{}) error
	All(result interface{}) error

	Cursor() Cursor
}

type aggregate struct {
	pipeline     interface{}
	hint         interface{}
	batch        *int32
	allowDiskUse *bool
	maxTime      *time.Duration

	ctx        context.Context
	collection *mongo.Collection
}

func (this *aggregate) Hint(hint interface{}) Aggregate {
	this.hint = hint
	return this
}

func (this *aggregate) BatchSize(n int32) Aggregate {
	this.batch = &n
	return this
}

func (this *aggregate) AllowDiskUse(b bool) Aggregate {
	this.allowDiskUse = &b
	return this
}

func (this *aggregate) MaxTime(maxTime time.Duration) Aggregate {
	this.maxTime = &maxTime
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
	var opts = options.Aggregate()

	if this.hint != nil {
		opts.SetHint(this.hint)
	}
	if this.batch != nil {
		opts.SetBatchSize(*this.batch)
	}
	if this.allowDiskUse != nil {
		opts.SetAllowDiskUse(*this.allowDiskUse)
	}
	if this.maxTime != nil {
		opts.SetMaxTime(*this.maxTime)
	}

	var cur, err = this.collection.Aggregate(this.ctx, this.pipeline, opts)
	return &cursor{Cursor: cur, err: err}
}
