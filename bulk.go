package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type WriteModel = mongo.WriteModel

type InsertOneModel = mongo.InsertOneModel

type DeleteOneModel = mongo.DeleteOneModel

type DeleteManyModel = mongo.DeleteManyModel

type ReplaceOneModel = mongo.ReplaceOneModel

type UpdateOneModel = mongo.UpdateOneModel

type UpdateManyModel = mongo.UpdateManyModel

func NewInsertOneModel() *InsertOneModel {
	return mongo.NewInsertOneModel()
}

func NewDeleteOneModel() *DeleteOneModel {
	return mongo.NewDeleteOneModel()
}

func NewDeleteManyModel() *DeleteManyModel {
	return mongo.NewDeleteManyModel()
}

func NewReplaceOneModel() *ReplaceOneModel {
	return mongo.NewReplaceOneModel()
}

func NewUpdateOneModel() *UpdateOneModel {
	return mongo.NewUpdateOneModel()
}

func NewUpdateManyModel() *UpdateManyModel {
	return mongo.NewUpdateManyModel()
}

type Bulk interface {
	Ordered(ordered bool) Bulk

	BypassDocumentValidation(bypass bool) Bulk

	AddModel(m WriteModel) Bulk

	InsertOne(document interface{}) Bulk

	InsertOneNx(filter interface{}, document interface{}) Bulk

	RepsertOne(filter interface{}, replacement interface{}) Bulk

	ReplaceOne(filter interface{}, replacement interface{}) Bulk

	UpsertOne(filter interface{}, update interface{}) Bulk

	UpsertId(id interface{}, update interface{}) Bulk

	Upsert(filter interface{}, update interface{}) Bulk

	UpdateOne(filter interface{}, update interface{}) Bulk

	UpdateId(id interface{}, update interface{}) Bulk

	UpdateMany(filter interface{}, update interface{}) Bulk

	DeleteOne(filter interface{}) Bulk

	DeleteId(id interface{}) Bulk

	DeleteMany(filter interface{}) Bulk

	Apply() (*BulkResult, error)
}

type bulk struct {
	models     []mongo.WriteModel
	ctx        context.Context
	opts       *options.BulkWriteOptions
	collection Collection
}

func (b *bulk) Ordered(ordered bool) Bulk {
	b.opts.SetOrdered(ordered)
	return b
}

func (b *bulk) BypassDocumentValidation(bypass bool) Bulk {
	b.opts.SetBypassDocumentValidation(bypass)
	return b
}

func (b *bulk) AddModel(m WriteModel) Bulk {
	if m != nil {
		b.models = append(b.models, m)
	}
	return b
}

func (b *bulk) InsertOne(document interface{}) Bulk {
	var m = NewInsertOneModel()
	m.SetDocument(document)
	return b.AddModel(m)
}

func (b *bulk) InsertOneNx(filter interface{}, document interface{}) Bulk {
	var m = NewUpdateOneModel()
	m.SetUpsert(true)
	m.SetFilter(filter)
	m.SetUpdate(M{"$setOnInsert": document})
	return b.AddModel(m)
}

func (b *bulk) RepsertOne(filter interface{}, replacement interface{}) Bulk {
	var m = NewReplaceOneModel()
	m.SetUpsert(true)
	m.SetFilter(filter)
	m.SetReplacement(replacement)
	return b.AddModel(m)
}

func (b *bulk) ReplaceOne(filter interface{}, replacement interface{}) Bulk {
	var m = NewReplaceOneModel()
	m.SetFilter(filter)
	m.SetReplacement(replacement)
	return b.AddModel(m)
}

func (b *bulk) UpsertOne(filter interface{}, update interface{}) Bulk {
	var m = NewUpdateOneModel()
	m.SetUpsert(true)
	m.SetFilter(filter)
	m.SetUpdate(update)
	return b.AddModel(m)
}

func (b *bulk) UpsertId(id interface{}, update interface{}) Bulk {
	return b.UpsertOne(M{"_id": id}, update)
}

func (b *bulk) Upsert(filter interface{}, update interface{}) Bulk {
	var m = NewUpdateManyModel()
	m.SetUpsert(true)
	m.SetFilter(filter)
	m.SetUpdate(update)
	return b.AddModel(m)
}

func (b *bulk) UpdateOne(filter interface{}, update interface{}) Bulk {
	var m = NewUpdateOneModel()
	m.SetFilter(filter)
	m.SetUpdate(update)
	return b.AddModel(m)
}

func (b *bulk) UpdateId(id interface{}, update interface{}) Bulk {
	return b.UpdateOne(M{"_id": id}, update)
}

func (b *bulk) UpdateMany(filter interface{}, update interface{}) Bulk {
	var m = NewUpdateManyModel()
	m.SetFilter(filter)
	m.SetUpdate(update)
	return b.AddModel(m)
}

func (b *bulk) DeleteOne(filter interface{}) Bulk {
	var m = NewDeleteOneModel()
	m.SetFilter(filter)
	return b.AddModel(m)
}

func (b *bulk) DeleteId(id interface{}) Bulk {
	return b.DeleteOne(M{"_id": id})
}

func (b *bulk) DeleteMany(filter interface{}) Bulk {
	var m = NewDeleteManyModel()
	m.SetFilter(filter)
	return b.AddModel(m)
}

func (b *bulk) Apply() (*BulkResult, error) {
	var result, err = b.collection.Collection().BulkWrite(b.ctx, b.models, b.opts)
	return result, err
}
