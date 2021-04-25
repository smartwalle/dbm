package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Collection interface {
	Name() string

	Database() *Database

	InsertOne(ctx context.Context, document interface{}) (*InsertOneResult, error)

	InsertMany(ctx context.Context, documents []interface{}) (*InsertManyResult, error)

	Insert(ctx context.Context, documents ...interface{}) (*InsertManyResult, error)

	ReplaceOne(ctx context.Context, filter interface{}, document interface{}) (*UpdateResult, error)

	Upsert(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error)

	UpsertId(ctx context.Context, id interface{}, update interface{}) (*UpdateResult, error)

	UpdateOne(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error)

	UpdateId(ctx context.Context, id interface{}, update interface{}) (*UpdateResult, error)

	UpdateMany(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error)

	DeleteOne(ctx context.Context, filter interface{}) (*DeleteResult, error)

	DeleteId(ctx context.Context, id interface{}) (*DeleteResult, error)

	DeleteMany(ctx context.Context, filter interface{}) (*DeleteResult, error)

	Find(ctx context.Context, filter interface{}) Query
}

type collection struct {
	*mongo.Collection
	database *Database
}

func (this *collection) Name() string {
	return this.Collection.Name()
}

func (this *collection) Database() *Database {
	return this.database
}

func (this *collection) InsertOne(ctx context.Context, document interface{}) (*InsertOneResult, error) {
	var opts = options.InsertOne()
	return this.Collection.InsertOne(ctx, document, opts)
}

func (this *collection) InsertMany(ctx context.Context, documents []interface{}) (*InsertManyResult, error) {
	var opts = options.InsertMany()
	return this.Collection.InsertMany(ctx, documents, opts)
}

func (this *collection) Insert(ctx context.Context, documents ...interface{}) (*InsertManyResult, error) {
	var opts = options.InsertMany()
	return this.Collection.InsertMany(ctx, documents, opts)
}

func (this *collection) ReplaceOne(ctx context.Context, filter interface{}, document interface{}) (*UpdateResult, error) {
	var opts = options.Replace()
	return this.Collection.ReplaceOne(ctx, filter, document, opts)
}

func (this *collection) Upsert(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Replace().SetUpsert(true)
	return this.Collection.ReplaceOne(ctx, filter, update, opts)
}

func (this *collection) UpsertId(ctx context.Context, id interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Replace().SetUpsert(true)
	return this.Collection.ReplaceOne(ctx, bson.M{"_id": id}, update, opts)
}

func (this *collection) UpdateOne(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Update()
	return this.Collection.UpdateOne(ctx, filter, update, opts)
}

func (this *collection) UpdateId(ctx context.Context, id interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Update()
	return this.Collection.UpdateByID(ctx, id, update, opts)
}

func (this *collection) UpdateMany(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Update()
	return this.Collection.UpdateMany(ctx, filter, update, opts)
}

func (this *collection) DeleteOne(ctx context.Context, filter interface{}) (*DeleteResult, error) {
	var opts = options.Delete()
	return this.Collection.DeleteOne(ctx, filter, opts)
}

func (this *collection) DeleteId(ctx context.Context, id interface{}) (*DeleteResult, error) {
	var opts = options.Delete()
	return this.Collection.DeleteOne(ctx, bson.M{"_id": id}, opts)
}

func (this *collection) DeleteMany(ctx context.Context, filter interface{}) (*DeleteResult, error) {
	var opts = options.Delete()
	return this.Collection.DeleteMany(ctx, filter, opts)
}

func (this *collection) Find(ctx context.Context, filter interface{}) Query {
	var q = &query{}
	q.ctx = ctx
	q.collection = this.Collection
	q.filter = filter
	return q
}
