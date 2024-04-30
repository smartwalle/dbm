package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CollectionOptions = options.CollectionOptions

func NewCollectionOptions() *CollectionOptions {
	return options.Collection()
}

type InsertOneOptions = options.InsertOneOptions

func NewInsertOneOptions() *InsertOneOptions {
	return options.InsertOne()
}

type InsertManyOptions = options.InsertManyOptions

func NewInsertManyOptions() *InsertManyOptions {
	return options.InsertMany()
}

type UpdateOptions = options.UpdateOptions

func NewUpdateOptions() *UpdateOptions {
	return options.Update()
}

type ReplaceOptions = options.ReplaceOptions

func NewReplaceOptions() *ReplaceOptions {
	return options.Replace()
}

type DeleteOptions = options.DeleteOptions

func NewDeleteOptions() *DeleteOptions {
	return options.Delete()
}

type Collection interface {
	Database() Database

	Collection() *mongo.Collection

	Name() string

	Drop(ctx context.Context) error

	Clone(opts ...*CollectionOptions) (Collection, error)

	IndexView() IndexView

	InsertOne(ctx context.Context, document interface{}, opts ...*InsertOneOptions) (*InsertOneResult, error)

	InsertOneNx(ctx context.Context, filter interface{}, document interface{}, opts ...*UpdateOptions) (*UpdateResult, error)

	InsertMany(ctx context.Context, documents []interface{}, opts ...*InsertManyOptions) (*InsertManyResult, error)

	Insert(ctx context.Context, documents ...interface{}) (*InsertManyResult, error)

	RepsertOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*ReplaceOptions) (*UpdateResult, error)

	ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*ReplaceOptions) (*UpdateResult, error)

	UpsertOne(ctx context.Context, filter interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error)

	UpsertId(ctx context.Context, id interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error)

	Upsert(ctx context.Context, filter interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error)

	UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error)

	UpdateId(ctx context.Context, id interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error)

	UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error)

	DeleteOne(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*DeleteResult, error)

	DeleteId(ctx context.Context, id interface{}, opts ...*DeleteOptions) (*DeleteResult, error)

	DeleteMany(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*DeleteResult, error)

	Find(ctx context.Context, filter interface{}) Query

	FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}) FindUpdate

	FindOneAndReplace(ctx context.Context, filter interface{}, replacement interface{}) FindReplace

	FindOneAndDelete(ctx context.Context, filter interface{}) FindDelete

	Bulk() Bulk

	Distinct(ctx context.Context, fieldName string, filter interface{}) Distinct

	Aggregate(ctx context.Context, pipeline interface{}) Aggregate

	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*ChangeStream, error)
}

type collection struct {
	collection *mongo.Collection
	database   Database
}

func (c *collection) Database() Database {
	return c.database
}

func (c *collection) Collection() *mongo.Collection {
	return c.collection
}

func (c *collection) Name() string {
	return c.collection.Name()
}

func (c *collection) Drop(ctx context.Context) error {
	return c.collection.Drop(ctx)
}

func (c *collection) Clone(opts ...*CollectionOptions) (Collection, error) {
	var nCollection, err = c.collection.Clone(opts...)
	if err != nil {
		return nil, err
	}
	return &collection{collection: nCollection, database: c.database}, nil
}

func (c *collection) IndexView() IndexView {
	var view = c.collection.Indexes()
	return &indexView{view: view}
}

func (c *collection) InsertOne(ctx context.Context, document interface{}, opts ...*InsertOneOptions) (*InsertOneResult, error) {
	return c.collection.InsertOne(ctx, document, opts...)
}

func (c *collection) InsertOneNx(ctx context.Context, filter interface{}, document interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	var opt = options.MergeUpdateOptions(opts...)
	opt.SetUpsert(true)
	// mongodb update 操作中，当 upsert 为 true 时，如果满足查询条件的记录存在，不会执行 $setOnInsert 中的操作
	return c.collection.UpdateOne(ctx, filter, bson.D{{"$setOnInsert", document}}, opt)
}

func (c *collection) InsertMany(ctx context.Context, documents []interface{}, opts ...*InsertManyOptions) (*InsertManyResult, error) {
	return c.collection.InsertMany(ctx, documents, opts...)
}

func (c *collection) Insert(ctx context.Context, documents ...interface{}) (*InsertManyResult, error) {
	var opts = options.InsertMany()
	return c.collection.InsertMany(ctx, documents, opts)
}

func (c *collection) RepsertOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*ReplaceOptions) (*UpdateResult, error) {
	var opt = options.MergeReplaceOptions(opts...)
	opt.SetUpsert(true)
	return c.collection.ReplaceOne(ctx, filter, replacement, opt)
}

func (c *collection) ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*ReplaceOptions) (*UpdateResult, error) {
	return c.collection.ReplaceOne(ctx, filter, replacement, opts...)
}

func (c *collection) UpsertOne(ctx context.Context, filter interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	var opt = options.MergeUpdateOptions(opts...)
	opt.SetUpsert(true)
	return c.collection.UpdateOne(ctx, filter, update, opt)
}

func (c *collection) UpsertId(ctx context.Context, id interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	var opt = options.MergeUpdateOptions(opts...)
	opt.SetUpsert(true)
	return c.collection.UpdateOne(ctx, bson.D{{"_id", id}}, update, opt)
}

func (c *collection) Upsert(ctx context.Context, filter interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	var opt = options.MergeUpdateOptions(opts...)
	opt.SetUpsert(true)
	return c.collection.UpdateMany(ctx, filter, update, opt)
}

func (c *collection) UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	return c.collection.UpdateOne(ctx, filter, update, opts...)
}

func (c *collection) UpdateId(ctx context.Context, id interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	return c.collection.UpdateByID(ctx, id, update, opts...)
}

func (c *collection) UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	return c.collection.UpdateMany(ctx, filter, update, opts...)
}

func (c *collection) DeleteOne(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*DeleteResult, error) {
	return c.collection.DeleteOne(ctx, filter, opts...)
}

func (c *collection) DeleteId(ctx context.Context, id interface{}, opts ...*DeleteOptions) (*DeleteResult, error) {
	return c.collection.DeleteOne(ctx, bson.D{{"_id", id}}, opts...)
}

func (c *collection) DeleteMany(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*DeleteResult, error) {
	return c.collection.DeleteMany(ctx, filter, opts...)
}

func (c *collection) Find(ctx context.Context, filter interface{}) Query {
	var q = &query{}
	q.ctx = ctx
	q.collection = c
	q.filter = filter
	return q
}

func (c *collection) FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}) FindUpdate {
	var q = &findUpdate{}
	q.filter = filter
	q.update = update
	q.ctx = ctx
	q.opts = options.FindOneAndUpdate()
	q.collection = c
	return q
}

func (c *collection) FindOneAndReplace(ctx context.Context, filter interface{}, replacement interface{}) FindReplace {
	var q = &findReplace{}
	q.filter = filter
	q.replacement = replacement
	q.ctx = ctx
	q.opts = options.FindOneAndReplace()
	q.collection = c
	return q
}

func (c *collection) FindOneAndDelete(ctx context.Context, filter interface{}) FindDelete {
	var q = &findDelete{}
	q.filter = filter
	q.ctx = ctx
	q.opts = options.FindOneAndDelete()
	q.collection = c
	return q
}

func (c *collection) Bulk() Bulk {
	var b = &bulk{}
	b.opts = options.BulkWrite()
	b.collection = c
	return b
}

func (c *collection) Distinct(ctx context.Context, fieldName string, filter interface{}) Distinct {
	var d = &distinct{}
	d.filter = filter
	d.fieldName = fieldName
	d.ctx = ctx
	d.opts = options.Distinct()
	d.collection = c
	return d
}

func (c *collection) Aggregate(ctx context.Context, pipeline interface{}) Aggregate {
	var a = &aggregate{}
	a.pipeline = pipeline
	a.ctx = ctx
	a.opts = options.Aggregate()
	a.aggregator = c.collection
	return a
}

func (c *collection) Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*ChangeStream, error) {
	return c.collection.Watch(ctx, pipeline, opts...)
}
