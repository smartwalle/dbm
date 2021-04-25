package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Collection struct {
	*mongo.Collection
	database *Database
}

func (this *Collection) Database() *Database {
	return this.database
}

func (this *Collection) InsertOne(ctx context.Context, document interface{}) (*InsertOneResult, error) {
	var opts = options.InsertOne()
	return this.Collection.InsertOne(ctx, document, opts)
}

func (this *Collection) InsertMany(ctx context.Context, documents []interface{}) (*InsertManyResult, error) {
	var opts = options.InsertMany()
	return this.Collection.InsertMany(ctx, documents, opts)
}

func (this *Collection) Insert(ctx context.Context, documents ...interface{}) (*InsertManyResult, error) {
	var opts = options.InsertMany()
	return this.Collection.InsertMany(ctx, documents, opts)
}

func (this *Collection) ReplaceOne(ctx context.Context, filter interface{}, document interface{}) (*UpdateResult, error) {
	var opts = options.Replace()
	return this.Collection.ReplaceOne(ctx, filter, document, opts)
}

func (this *Collection) Upsert(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Replace().SetUpsert(true)
	return this.Collection.ReplaceOne(ctx, filter, update, opts)
}

func (this *Collection) UpsertId(ctx context.Context, id interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Replace().SetUpsert(true)
	return this.Collection.ReplaceOne(ctx, bson.M{"_id": id}, update, opts)
}

func (this *Collection) UpdateOne(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Update()
	return this.Collection.UpdateOne(ctx, filter, update, opts)
}

func (this *Collection) UpdateId(ctx context.Context, id interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Update()
	return this.Collection.UpdateByID(ctx, id, update, opts)
}

func (this *Collection) UpdateMany(ctx context.Context, filter interface{}, update interface{}) (*UpdateResult, error) {
	var opts = options.Update()
	return this.Collection.UpdateMany(ctx, filter, update, opts)
}

func (this *Collection) DeleteOne(ctx context.Context, filter interface{}) (*DeleteResult, error) {
	var opts = options.Delete()
	return this.Collection.DeleteOne(ctx, filter, opts)
}

func (this *Collection) DeleteId(ctx context.Context, id interface{}) (*DeleteResult, error) {
	var opts = options.Delete()
	return this.Collection.DeleteOne(ctx, bson.M{"_id": id}, opts)
}

func (this *Collection) DeleteMany(ctx context.Context, filter interface{}) (*DeleteResult, error) {
	var opts = options.Delete()
	return this.Collection.DeleteMany(ctx, filter, opts)
}

func (this *Collection) Find(ctx context.Context, filter interface{}) Query {
	var q = &query{}
	q.ctx = ctx
	q.collection = this.Collection
	q.filter = filter
	return q
}
