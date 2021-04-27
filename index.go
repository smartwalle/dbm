package dbm

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type IndexOptions = options.IndexOptions

func NewIndexOptions() *IndexOptions {
	return &IndexOptions{}
}

type IndexView interface {
	IndexView() mongo.IndexView

	Create(ctx context.Context, keys []string, opts *IndexOptions) (string, error)

	CreateIndex(ctx context.Context, name string, keys []string) (string, error)

	CreateUniqueIndex(ctx context.Context, name string, keys []string) (string, error)

	CreateTTLIndex(ctx context.Context, name string, keys []string, ttl int32) (string, error)

	DropIndex(ctx context.Context, keys []string) error

	Drop(ctx context.Context, name string) error

	DropAll(ctx context.Context) error
}

type indexView struct {
	view mongo.IndexView
}

func (this *indexView) IndexView() mongo.IndexView {
	return this.view
}

func (this *indexView) Create(ctx context.Context, keys []string, opts *IndexOptions) (string, error) {
	var model = mongo.IndexModel{}
	model.Keys = parseIndexKey(keys)
	model.Options = opts
	return this.view.CreateOne(ctx, model)
}

func (this *indexView) CreateIndex(ctx context.Context, name string, keys []string) (string, error) {
	var opts = NewIndexOptions()
	opts.SetName(name)
	return this.Create(ctx, keys, opts)
}

func (this *indexView) CreateUniqueIndex(ctx context.Context, name string, keys []string) (string, error) {
	var opts = NewIndexOptions()
	opts.SetName(name)
	opts.SetUnique(true)
	return this.Create(ctx, keys, opts)
}

func (this *indexView) CreateTTLIndex(ctx context.Context, name string, keys []string, ttl int32) (string, error) {
	var opts = NewIndexOptions()
	opts.SetName(name)
	opts.SetExpireAfterSeconds(ttl)
	return this.Create(ctx, keys, opts)
}

func parseIndexKey(keys []string) bson.D {
	var doc bson.D
	for _, field := range keys {
		var sort = int32(1)
		field, sort = sortField(field)
		doc = append(doc, bson.E{field, sort})
	}
	return doc
}

func (this *indexView) DropIndex(ctx context.Context, keys []string) error {
	var name string
	for _, key := range keys {
		field, sort := sortField(key)
		field = field + "_" + fmt.Sprint(sort)

		if name == "" {
			name = field
		} else {
			name += "_" + field
		}
	}
	_, err := this.view.DropOne(ctx, name)
	return err
}

func (this *indexView) Drop(ctx context.Context, name string) error {
	_, err := this.view.DropOne(ctx, name)
	return err
}

func (this *indexView) DropAll(ctx context.Context) error {
	_, err := this.view.DropAll(ctx)
	return err
}
