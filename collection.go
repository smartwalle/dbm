package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Collection interface {
	Database() Database

	Collection() *mongo.Collection

	Name() string

	Drop(ctx context.Context) error

	Clone() (Collection, error)

	IndexView() IndexView

	InsertOne(ctx context.Context, document interface{}) (*InsertOneResult, error)

	InsertOneNx(ctx context.Context, filter interface{}, document interface{}) (*UpdateResult, error)

	InsertMany(ctx context.Context, documents []interface{}) (*InsertManyResult, error)

	Insert(ctx context.Context, documents ...interface{}) (*InsertManyResult, error)

	ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}) (*UpdateResult, error)

	Upsert(ctx context.Context, filter interface{}, replacement interface{}) (*UpdateResult, error)

	UpsertId(ctx context.Context, id interface{}, replacement interface{}) (*UpdateResult, error)

	UpdateOne(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error)

	UpdateId(ctx context.Context, id interface{}, update interface{}) (*UpdateResult, error)

	UpdateMany(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error)

	DeleteOne(ctx context.Context, filter interface{}) (*DeleteResult, error)

	DeleteId(ctx context.Context, id interface{}) (*DeleteResult, error)

	DeleteMany(ctx context.Context, filter interface{}) (*DeleteResult, error)

	Find(ctx context.Context, filter interface{}) Query

	FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}) FindUpdate

	FindOneAndReplace(ctx context.Context, filter interface{}, replacement interface{}) FindReplace

	FindOneAndDelete(ctx context.Context, filter interface{}) FindDelete

	Distinct(ctx context.Context, fieldName string, filter interface{}) Distinct

	Bulk(ctx context.Context) Bulk

	Aggregate(ctx context.Context, pipeline interface{}) Aggregate

	Watch(ctx context.Context, pipeline interface{}) Watcher
}

type collection struct {
	collection *mongo.Collection
	database   Database
}

func (this *collection) Database() Database {
	return this.database
}

func (this *collection) Collection() *mongo.Collection {
	return this.collection
}

func (this *collection) Name() string {
	return this.collection.Name()
}

func (this *collection) Drop(ctx context.Context) error {
	return this.collection.Drop(ctx)
}

func (this *collection) Clone() (Collection, error) {
	var nCollection, err = this.collection.Clone()
	if err != nil {
		return nil, err
	}
	return &collection{collection: nCollection, database: this.database}, nil
}

func (this *collection) IndexView() IndexView {
	var view = this.collection.Indexes()
	return &indexView{view: view}
}

func (this *collection) InsertOne(ctx context.Context, document interface{}) (*InsertOneResult, error) {
	var opts = options.InsertOne()
	return this.collection.InsertOne(ctx, document, opts)
}

func (this *collection) InsertOneNx(ctx context.Context, filter interface{}, document interface{}) (*UpdateResult, error) {
	var opts = options.Update().SetUpsert(true)
	return this.collection.UpdateOne(ctx, filter, bson.M{"$setOnInsert": document}, opts)
}

func (this *collection) InsertMany(ctx context.Context, documents []interface{}) (*InsertManyResult, error) {
	var opts = options.InsertMany()
	return this.collection.InsertMany(ctx, documents, opts)
}

func (this *collection) Insert(ctx context.Context, documents ...interface{}) (*InsertManyResult, error) {
	var opts = options.InsertMany()
	return this.collection.InsertMany(ctx, documents, opts)
}

func (this *collection) ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}) (*UpdateResult, error) {
	var opts = options.Replace()
	return this.collection.ReplaceOne(ctx, filter, replacement, opts)
}

func (this *collection) Upsert(ctx context.Context, filter interface{}, replacement interface{}) (*UpdateResult, error) {
	var opts = options.Replace().SetUpsert(true)
	return this.collection.ReplaceOne(ctx, filter, replacement, opts)
}

func (this *collection) UpsertId(ctx context.Context, id interface{}, replacement interface{}) (*UpdateResult, error) {
	var opts = options.Replace().SetUpsert(true)
	return this.collection.ReplaceOne(ctx, bson.M{"_id": id}, replacement, opts)
}

func (this *collection) UpdateOne(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Update()
	return this.collection.UpdateOne(ctx, filter, update, opts)
}

func (this *collection) UpdateId(ctx context.Context, id interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Update()
	return this.collection.UpdateByID(ctx, id, update, opts)
}

func (this *collection) UpdateMany(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Update()
	return this.collection.UpdateMany(ctx, filter, update, opts)
}

func (this *collection) DeleteOne(ctx context.Context, filter interface{}) (*DeleteResult, error) {
	var opts = options.Delete()
	return this.collection.DeleteOne(ctx, filter, opts)
}

func (this *collection) DeleteId(ctx context.Context, id interface{}) (*DeleteResult, error) {
	var opts = options.Delete()
	return this.collection.DeleteOne(ctx, bson.M{"_id": id}, opts)
}

func (this *collection) DeleteMany(ctx context.Context, filter interface{}) (*DeleteResult, error) {
	var opts = options.Delete()
	return this.collection.DeleteMany(ctx, filter, opts)
}

func (this *collection) Find(ctx context.Context, filter interface{}) Query {
	var q = &query{}
	q.ctx = ctx
	q.collection = this
	q.filter = filter
	return q
}

func (this *collection) FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}) FindUpdate {
	var q = &findUpdate{}
	q.filter = filter
	q.update = update
	q.ctx = ctx
	q.opts = options.FindOneAndUpdate()
	q.collection = this
	return q
}

func (this *collection) FindOneAndReplace(ctx context.Context, filter interface{}, replacement interface{}) FindReplace {
	var q = &findReplace{}
	q.filter = filter
	q.replacement = replacement
	q.ctx = ctx
	q.opts = options.FindOneAndReplace()
	q.collection = this
	return q
}

func (this *collection) FindOneAndDelete(ctx context.Context, filter interface{}) FindDelete {
	var q = &findDelete{}
	q.filter = filter
	q.ctx = ctx
	q.opts = options.FindOneAndDelete()
	q.collection = this
	return q
}

func (this *collection) Distinct(ctx context.Context, fieldName string, filter interface{}) Distinct {
	var d = &distinct{}
	d.filter = filter
	d.fieldName = fieldName
	d.ctx = ctx
	d.opts = options.Distinct()
	d.collection = this
	return d
}

func (this *collection) Bulk(ctx context.Context) Bulk {
	var b = &bulk{}
	b.ctx = ctx
	b.opts = options.BulkWrite()
	b.collection = this
	return b
}

func (this *collection) Aggregate(ctx context.Context, pipeline interface{}) Aggregate {
	var a = &aggregate{}
	a.pipeline = pipeline
	a.ctx = ctx
	a.opts = options.Aggregate()
	a.aggregator = this.collection
	return a
}

func (this *collection) Watch(ctx context.Context, pipeline interface{}) Watcher {
	var w = &watch{}
	w.pipeline = pipeline
	w.ctx = ctx
	w.opts = options.ChangeStream()
	w.watcher = this.collection
	return w
}
