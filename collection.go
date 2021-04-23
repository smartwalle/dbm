package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
)

type Collection struct {
	*mongo.Collection
	database *Database
}

func (this *Collection) Database() *Database {
	return this.database
}

func (this *Collection) InsertOne(ctx context.Context, document interface{}) (*InsertOneResult, error) {
	return this.Collection.InsertOne(ctx, document)
}

func (this *Collection) InsertMany(ctx context.Context, documents []interface{}) (*InsertManyResult, error) {
	return this.Collection.InsertMany(ctx, documents)
}
