package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
)

type Query interface {
	Sort(fields ...string) Query
	Select(selector interface{}) Query
	Skip(n int64) Query
	Limit(n int64) Query
	Hint(hint interface{}) Query

	One(result interface{}) error
	All(result interface{}) error
	Count() (int64, error)

	Cursor() Cursor
}

type query struct {
	filter  interface{}
	sort    interface{}
	project interface{}
	hint    interface{}
	limit   *int64
	skip    *int64

	ctx        context.Context
	collection *mongo.Collection
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
			panic("Sort: empty field name")
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

func (this *query) Select(projection interface{}) Query {
	this.project = projection
	return this
}

func (this *query) Skip(n int64) Query {
	this.skip = &n
	return this
}

func (this *query) Limit(n int64) Query {
	this.limit = &n
	return this
}

func (this *query) Hint(hint interface{}) Query {
	this.hint = hint
	return this
}

func (this *query) One(result interface{}) error {
	var opts = options.FindOne()

	if this.sort != nil {
		opts.SetSort(this.sort)
	}
	if this.project != nil {
		opts.SetProjection(this.project)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}
	if this.hint != nil {
		opts.SetHint(this.hint)
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

	if this.sort != nil {
		opts.SetSort(this.sort)
	}
	if this.project != nil {
		opts.SetProjection(this.project)
	}
	if this.limit != nil {
		opts.SetLimit(*this.limit)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}
	if this.hint != nil {
		opts.SetHint(this.hint)
	}

	var cur, err = this.collection.Find(this.ctx, this.filter, opts)
	return &cursor{Cursor: cur, err: err}
}
