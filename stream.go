package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type watcher interface {
	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

type Watcher interface {
	BatchSize(n int32) Watcher

	Collation(c Collation) Watcher

	FullDocument(fd FullDocument) Watcher

	MaxAwaitTime(d time.Duration) Watcher

	ResumeAfter(rt interface{}) Watcher

	StartAtOperationTime(t *Timestamp) Watcher

	StartAfter(sa interface{}) Watcher

	Stream() Stream
}

type watch struct {
	pipeline interface{}

	ctx     context.Context
	opts    *options.ChangeStreamOptions
	watcher watcher
}

func (this *watch) BatchSize(n int32) Watcher {
	this.opts.SetBatchSize(n)
	return this
}

func (this *watch) Collation(c Collation) Watcher {
	this.opts.SetCollation(c)
	return this
}

func (this *watch) FullDocument(fd FullDocument) Watcher {
	this.opts.SetFullDocument(fd)
	return this
}

func (this *watch) MaxAwaitTime(d time.Duration) Watcher {
	this.opts.SetMaxAwaitTime(d)
	return this
}

func (this *watch) ResumeAfter(rt interface{}) Watcher {
	this.opts.SetResumeAfter(rt)
	return this
}

func (this *watch) StartAtOperationTime(t *Timestamp) Watcher {
	this.opts.SetStartAtOperationTime(t)
	return this
}

func (this *watch) StartAfter(sa interface{}) Watcher {
	this.opts.SetStartAfter(sa)
	return this
}

func (this *watch) Stream() Stream {
	var s, err = this.watcher.Watch(this.ctx, this.pipeline, this.opts)
	var ns = &stream{}
	ns.ChangeStream = s
	ns.err = err
	return ns
}

type Stream interface {
	ID() int64

	ResumeToken() Raw

	Next(ctx context.Context) bool

	TryNext(ctx context.Context) bool

	One(result interface{}) error

	Close(ctx context.Context) error

	Error() error
}

type stream struct {
	*mongo.ChangeStream
	err error
}

func (this *stream) ID() int64 {
	if this.err != nil {
		return 0
	}
	return this.ChangeStream.ID()
}

func (this *stream) ResumeToken() Raw {
	if this.err != nil {
		return nil
	}
	return this.ChangeStream.ResumeToken()
}

func (this *stream) Next(ctx context.Context) bool {
	if this.err != nil {
		return false
	}
	return this.ChangeStream.Next(ctx)
}

func (this *stream) TryNext(ctx context.Context) bool {
	if this.err != nil {
		return false
	}
	return this.ChangeStream.TryNext(ctx)
}

func (this *stream) One(result interface{}) error {
	if this.err != nil {
		return this.err
	}
	return this.ChangeStream.Decode(result)
}

func (this *stream) Close(ctx context.Context) error {
	if this.err != nil {
		return this.err
	}
	return this.ChangeStream.Close(ctx)
}

func (this *stream) Error() error {
	if this.err != nil {
		return this.err
	}
	return this.ChangeStream.Err()
}
