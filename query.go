package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
)

type Query interface {
	Select(selector interface{}) Query
	Sort(fields ...string) Query
	Hint(hint interface{}) Query
	Limit(n int64) Query
	Skip(n int64) Query
	BatchSize(n int32) Query

	One(result interface{}) error
	All(result interface{}) error
	Count() (int64, error)

	Cursor() Cursor
}

type query struct {
	filter  interface{}
	project interface{}
	sort    interface{}
	hint    interface{}
	limit   *int64
	skip    *int64
	batch   *int32

	ctx        context.Context
	collection *mongo.Collection
}

func (this *query) Select(projection interface{}) Query {
	this.project = projection
	return this
}

func (this *query) Sort(fields ...string) Query {
	if len(fields) == 0 {
		return this
	}

	var sorts bson.D
	for _, field := range fields {
		n := 1
		var kind string
		if field != "" {
			if field[0] == '$' {
				if c := strings.Index(field, ":"); c > 1 && c < len(field)-1 {
					kind = field[1:c]
					field = field[c+1:]
				}
			}
			switch field[0] {
			case '+':
				field = field[1:]
			case '-':
				n = -1
				field = field[1:]
			}
		}
		if field == "" {
			continue
		}
		if kind == "textScore" {
			sorts = append(sorts, bson.E{Key: field, Value: bson.M{"$meta": kind}})
		} else {
			sorts = append(sorts, bson.E{Key: field, Value: n})
		}
	}
	this.sort = sorts
	return this
}

func (this *query) Hint(hint interface{}) Query {
	this.hint = hint
	return this
}

func (this *query) Limit(n int64) Query {
	this.limit = &n
	return this
}

func (this *query) Skip(n int64) Query {
	this.skip = &n
	return this
}

func (this *query) BatchSize(n int32) Query {
	this.batch = &n
	return this
}

func (this *query) One(result interface{}) error {
	var opts = options.FindOne()
	if this.project != nil {
		opts.SetProjection(this.project)
	}
	if this.sort != nil {
		opts.SetSort(this.sort)
	}
	if this.hint != nil {
		opts.SetHint(this.hint)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}
	return this.collection.FindOne(this.ctx, this.filter, opts).Decode(result)
}

func (this *query) All(result interface{}) error {
	var cur = this.Cursor()
	return cur.All(this.ctx, result)
}

func (this *query) Count() (n int64, err error) {
	var opts = options.Count()

	if this.limit != nil {
		opts.SetLimit(*this.limit)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}
	return this.collection.CountDocuments(this.ctx, this.filter, opts)
}

func (this *query) Cursor() Cursor {
	var opts = options.Find()

	if this.project != nil {
		opts.SetProjection(this.project)
	}
	if this.sort != nil {
		opts.SetSort(this.sort)
	}
	if this.hint != nil {
		opts.SetHint(this.hint)
	}
	if this.limit != nil {
		opts.SetLimit(*this.limit)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}
	if this.batch != nil {
		opts.SetBatchSize(*this.batch)
	}

	var cur, err = this.collection.Find(this.ctx, this.filter, opts)
	return &cursor{Cursor: cur, err: err}
}
