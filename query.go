package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"time"
)

type Collation = options.Collation

type Query interface {
	BatchSize(n int32) Query

	Hint(hint interface{}) Query

	Limit(n int64) Query

	Select(selector interface{}) Query

	Skip(n int64) Query

	Sort(fields ...string) Query

	AllowDiskUse(b bool) Query

	AllowPartialResults(b bool) Query

	Collation(c *Collation) Query

	Comment(s string) Query

	Max(m interface{}) Query

	MaxAwaitTime(d time.Duration) Query

	MaxTime(d time.Duration) Query

	Min(m interface{}) Query

	NoCursorTimeout(b bool) Query

	ReturnKey(b bool) Query

	ShowRecordId(b bool) Query

	One(result interface{}) error

	All(result interface{}) error

	Count() (int64, error)

	Cursor() Cursor
}

type query struct {
	filter interface{}

	allowDiskUse        *bool
	allowPartialResults *bool
	batchSize           *int32
	collation           *Collation
	comment             *string
	cursorType          *options.CursorType
	hint                interface{}
	limit               *int64
	max                 interface{}
	maxAwaitTime        *time.Duration
	maxTime             *time.Duration
	min                 interface{}
	noCursorTimeout     *bool
	projection          interface{}
	returnKey           *bool
	showRecordID        *bool
	skip                *int64
	sort                interface{}

	ctx        context.Context
	collection *mongo.Collection
}

func (this *query) AllowDiskUse(b bool) Query {
	this.allowDiskUse = &b
	return this
}

func (this *query) AllowPartialResults(b bool) Query {
	this.allowPartialResults = &b
	return this
}

func (this *query) BatchSize(n int32) Query {
	this.batchSize = &n
	return this
}

func (this *query) Collation(c *Collation) Query {
	this.collation = c
	return this
}

func (this *query) Comment(s string) Query {
	this.comment = &s
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

func (this *query) Max(m interface{}) Query {
	this.max = m
	return this
}

func (this *query) MaxAwaitTime(d time.Duration) Query {
	this.maxAwaitTime = &d
	return this
}

func (this *query) MaxTime(d time.Duration) Query {
	this.maxTime = &d
	return this
}

func (this *query) Min(m interface{}) Query {
	this.min = m
	return this
}

func (this *query) NoCursorTimeout(b bool) Query {
	this.noCursorTimeout = &b
	return this
}

func (this *query) Select(projection interface{}) Query {
	this.projection = projection
	return this
}

func (this *query) ReturnKey(b bool) Query {
	this.returnKey = &b
	return this
}

func (this *query) ShowRecordId(b bool) Query {
	this.showRecordID = &b
	return this
}

func (this *query) Skip(n int64) Query {
	this.skip = &n
	return this
}

func (this *query) Sort(fields ...string) Query {
	if len(fields) == 0 {
		return this
	}

	var sorts bson.D
	for _, field := range fields {
		var sort = int32(1)
		var kind string
		if field != "" {
			if field[0] == '$' {
				if c := strings.Index(field, ":"); c > 1 && c < len(field)-1 {
					kind = field[1:c]
					field = field[c+1:]
				}
			}
			//switch field[0] {
			//case '+':
			//	field = field[1:]
			//case '-':
			//	sort = -1
			//	field = field[1:]
			//}
			field, sort = SortField(field)
		}
		if field == "" {
			continue
		}
		if kind == "textScore" {
			sorts = append(sorts, bson.E{Key: field, Value: bson.M{"$meta": kind}})
		} else {
			sorts = append(sorts, bson.E{Key: field, Value: sort})
		}
	}
	this.sort = sorts
	return this
}

func SortField(field string) (key string, sort int32) {
	sort = 1
	key = field

	if len(field) != 0 {
		switch field[0] {
		case '+':
			sort = 1
			key = field[1:]
		case '-':
			sort = -1
			key = field[1:]
		}
	}
	return key, sort
}

func (this *query) One(result interface{}) error {
	var opts = options.FindOne()

	if this.allowPartialResults != nil {
		opts.SetAllowPartialResults(*this.allowPartialResults)
	}
	if this.collation != nil {
		opts.SetCollation(this.collation)
	}
	if this.comment != nil {
		opts.SetComment(*this.comment)
	}
	if this.hint != nil {
		opts.SetHint(this.hint)
	}
	if this.max != nil {
		opts.SetMax(this.max)
	}
	if this.maxTime != nil {
		opts.SetMaxTime(*this.maxTime)
	}
	if this.min != nil {
		opts.SetMin(this.min)
	}
	if this.projection != nil {
		opts.SetProjection(this.projection)
	}
	if this.returnKey != nil {
		opts.SetReturnKey(*this.returnKey)
	}
	if this.showRecordID != nil {
		opts.SetShowRecordID(*this.showRecordID)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}
	if this.sort != nil {
		opts.SetSort(this.sort)
	}

	return this.collection.FindOne(this.ctx, this.filter, opts).Decode(result)
}

func (this *query) All(result interface{}) error {
	var cur = this.Cursor()
	defer cur.Close(this.ctx)
	return cur.All(this.ctx, result)
}

func (this *query) Count() (n int64, err error) {
	var opts = options.Count()

	if this.collation != nil {
		opts.SetCollation(this.collation)
	}
	if this.hint != nil {
		opts.SetHint(this.hint)
	}
	if this.limit != nil {
		opts.SetLimit(*this.limit)
	}
	if this.maxTime != nil {
		opts.SetMaxTime(*this.maxTime)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}

	return this.collection.CountDocuments(this.ctx, this.filter, opts)
}

func (this *query) Cursor() Cursor {
	var opts = options.Find()

	if this.allowDiskUse != nil {
		opts.SetAllowDiskUse(*this.allowDiskUse)
	}
	if this.allowPartialResults != nil {
		opts.SetAllowPartialResults(*this.allowPartialResults)
	}
	if this.batchSize != nil {
		opts.SetBatchSize(*this.batchSize)
	}
	if this.collation != nil {
		opts.SetCollation(this.collation)
	}
	if this.comment != nil {
		opts.SetComment(*this.comment)
	}
	if this.hint != nil {
		opts.SetHint(this.hint)
	}
	if this.limit != nil {
		opts.SetLimit(*this.limit)
	}
	if this.max != nil {
		opts.SetMax(this.max)
	}
	if this.maxAwaitTime != nil {
		opts.SetMaxAwaitTime(*this.maxAwaitTime)
	}
	if this.maxTime != nil {
		opts.SetMaxTime(*this.maxTime)
	}
	if this.min != nil {
		opts.SetMin(this.min)
	}
	if this.noCursorTimeout != nil {
		opts.SetNoCursorTimeout(*this.noCursorTimeout)
	}
	if this.projection != nil {
		opts.SetProjection(this.projection)
	}
	if this.returnKey != nil {
		opts.SetReturnKey(*this.returnKey)
	}
	if this.showRecordID != nil {
		opts.SetShowRecordID(*this.showRecordID)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}
	if this.sort != nil {
		opts.SetSort(this.sort)
	}

	var cur, err = this.collection.Find(this.ctx, this.filter, opts)
	return &cursor{Cursor: cur, err: err}
}
