package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

/*
type UserEvent struct {
	dbm.ChangeEvent    `bson:",inline"`
	FullDocument User `bson:"fullDocument"`
	DocumentKey  struct {
		Id string `bson:"_id"`
	} `bson:"documentKey"`
}

var stream = collection.Watch(context.Background(), dbm.NP()).FullDocument(options.UpdateLookup).Stream()
for stream.Next(context.Background()) {
	var uEvent *UserEvent
	stream.One(&uEvent)
}
*/

type OperationType string

const (
	OperationTypeInsert     = "insert"
	OperationTypeDelete     = "delete"
	OperationTypeReplace    = "replace"
	OperationTypeUpdate     = "update"
	OperationTypeInvalidate = "invalidate"
)

type ChangeEvent struct {
	Id            EventId       `bson:"_id"`
	OperationType OperationType `bson:"operationType"`
	ClusterTime   Timestamp     `bson:"clusterTime"`
	Namespace     Namespace     `bson:"ns"`
}

type EventId struct {
	Data string `bson:"_data"`
}

type Namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

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

func (w *watch) BatchSize(n int32) Watcher {
	w.opts.SetBatchSize(n)
	return w
}

func (w *watch) Collation(c Collation) Watcher {
	w.opts.SetCollation(c)
	return w
}

func (w *watch) FullDocument(fd FullDocument) Watcher {
	w.opts.SetFullDocument(fd)
	return w
}

func (w *watch) MaxAwaitTime(d time.Duration) Watcher {
	w.opts.SetMaxAwaitTime(d)
	return w
}

func (w *watch) ResumeAfter(rt interface{}) Watcher {
	w.opts.SetResumeAfter(rt)
	return w
}

func (w *watch) StartAtOperationTime(t *Timestamp) Watcher {
	w.opts.SetStartAtOperationTime(t)
	return w
}

func (w *watch) StartAfter(sa interface{}) Watcher {
	w.opts.SetStartAfter(sa)
	return w
}

func (w *watch) Stream() Stream {
	var s, err = w.watcher.Watch(w.ctx, w.pipeline, w.opts)
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

func (s *stream) ID() int64 {
	if s.err != nil {
		return 0
	}
	return s.ChangeStream.ID()
}

func (s *stream) ResumeToken() Raw {
	if s.err != nil {
		return nil
	}
	return s.ChangeStream.ResumeToken()
}

func (s *stream) Next(ctx context.Context) bool {
	if s.err != nil {
		return false
	}
	return s.ChangeStream.Next(ctx)
}

func (s *stream) TryNext(ctx context.Context) bool {
	if s.err != nil {
		return false
	}
	return s.ChangeStream.TryNext(ctx)
}

func (s *stream) One(result interface{}) error {
	if s.err != nil {
		return s.err
	}
	return s.ChangeStream.Decode(result)
}

func (s *stream) Close(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}
	return s.ChangeStream.Close(ctx)
}

func (s *stream) Error() error {
	if s.err != nil {
		return s.err
	}
	return s.ChangeStream.Err()
}
