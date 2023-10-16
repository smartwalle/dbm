package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"time"
)

type Collation = options.Collation

type ArrayFilters = options.ArrayFilters

type ReturnDocument = options.ReturnDocument

type FullDocument = options.FullDocument

const (
	Before = options.Before
	After  = options.After
)

const (
	Default      = options.Default
	UpdateLookup = options.UpdateLookup
)

type CursorType = options.CursorType

const (
	NonTailable   = options.NonTailable
	Tailable      = options.Tailable
	TailableAwait = options.TailableAwait
)

type Query interface {
	BatchSize(n int32) Query

	Hint(hint interface{}) Query

	Limit(n int64) Query

	Project(projection interface{}) Query
	Select(projection interface{}) Query

	Skip(n int64) Query

	Sort(fields ...string) Query

	AllowDiskUse(b bool) Query

	AllowPartialResults(b bool) Query

	Collation(c *Collation) Query

	Comment(s string) Query

	CursorType(cursorType CursorType) Query

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
	showRecordId        *bool
	skip                *int64
	sort                interface{}

	ctx        context.Context
	collection Collection
}

func (q *query) AllowDiskUse(b bool) Query {
	q.allowDiskUse = &b
	return q
}

func (q *query) AllowPartialResults(b bool) Query {
	q.allowPartialResults = &b
	return q
}

func (q *query) BatchSize(n int32) Query {
	q.batchSize = &n
	return q
}

func (q *query) Collation(c *Collation) Query {
	q.collation = c
	return q
}

func (q *query) Comment(s string) Query {
	q.comment = &s
	return q
}

func (q *query) CursorType(cursorType CursorType) Query {
	q.cursorType = &cursorType
	return q
}

func (q *query) Hint(hint interface{}) Query {
	q.hint = hint
	return q
}

func (q *query) Limit(n int64) Query {
	q.limit = &n
	return q
}

func (q *query) Max(m interface{}) Query {
	q.max = m
	return q
}

func (q *query) MaxAwaitTime(d time.Duration) Query {
	q.maxAwaitTime = &d
	return q
}

func (q *query) MaxTime(d time.Duration) Query {
	q.maxTime = &d
	return q
}

func (q *query) Min(m interface{}) Query {
	q.min = m
	return q
}

func (q *query) NoCursorTimeout(b bool) Query {
	q.noCursorTimeout = &b
	return q
}

func (q *query) Project(projection interface{}) Query {
	q.projection = projection
	return q
}

func (q *query) Select(projection interface{}) Query {
	q.projection = projection
	return q
}

func (q *query) ReturnKey(b bool) Query {
	q.returnKey = &b
	return q
}

func (q *query) ShowRecordId(b bool) Query {
	q.showRecordId = &b
	return q
}

func (q *query) Skip(n int64) Query {
	q.skip = &n
	return q
}

func (q *query) Sort(fields ...string) Query {
	if len(fields) == 0 {
		return q
	}
	q.sort = sortFields(fields...)
	return q
}

func sortFields(fields ...string) bson.D {
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
			field, sort = sortField(field)
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
	return sorts
}

func sortField(field string) (key string, sort int32) {
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

func (q *query) One(result interface{}) error {
	var opts = options.FindOne()

	if q.allowPartialResults != nil {
		opts.SetAllowPartialResults(*q.allowPartialResults)
	}
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.comment != nil {
		opts.SetComment(*q.comment)
	}
	if q.hint != nil {
		opts.SetHint(q.hint)
	}
	if q.max != nil {
		opts.SetMax(q.max)
	}
	if q.maxTime != nil {
		opts.SetMaxTime(*q.maxTime)
	}
	if q.min != nil {
		opts.SetMin(q.min)
	}
	if q.projection != nil {
		opts.SetProjection(q.projection)
	}
	if q.returnKey != nil {
		opts.SetReturnKey(*q.returnKey)
	}
	if q.showRecordId != nil {
		opts.SetShowRecordID(*q.showRecordId)
	}
	if q.skip != nil {
		opts.SetSkip(*q.skip)
	}
	if q.sort != nil {
		opts.SetSort(q.sort)
	}

	return q.collection.Collection().FindOne(q.ctx, q.filter, opts).Decode(result)
}

func (q *query) All(result interface{}) error {
	var cur = q.Cursor()
	defer cur.Close(q.ctx)
	return cur.All(q.ctx, result)
}

func (q *query) Count() (n int64, err error) {
	var opts = options.Count()

	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.hint != nil {
		opts.SetHint(q.hint)
	}
	if q.limit != nil {
		opts.SetLimit(*q.limit)
	}
	if q.maxTime != nil {
		opts.SetMaxTime(*q.maxTime)
	}
	if q.skip != nil {
		opts.SetSkip(*q.skip)
	}

	return q.collection.Collection().CountDocuments(q.ctx, q.filter, opts)
}

func (q *query) Cursor() Cursor {
	var opts = options.Find()

	if q.allowDiskUse != nil {
		opts.SetAllowDiskUse(*q.allowDiskUse)
	}
	if q.allowPartialResults != nil {
		opts.SetAllowPartialResults(*q.allowPartialResults)
	}
	if q.batchSize != nil {
		opts.SetBatchSize(*q.batchSize)
	}
	if q.collation != nil {
		opts.SetCollation(q.collation)
	}
	if q.comment != nil {
		opts.SetComment(*q.comment)
	}
	if q.cursorType != nil {
		opts.SetCursorType(*q.cursorType)
	}
	if q.hint != nil {
		opts.SetHint(q.hint)
	}
	if q.limit != nil {
		opts.SetLimit(*q.limit)
	}
	if q.max != nil {
		opts.SetMax(q.max)
	}
	if q.maxAwaitTime != nil {
		opts.SetMaxAwaitTime(*q.maxAwaitTime)
	}
	if q.maxTime != nil {
		opts.SetMaxTime(*q.maxTime)
	}
	if q.min != nil {
		opts.SetMin(q.min)
	}
	if q.noCursorTimeout != nil {
		opts.SetNoCursorTimeout(*q.noCursorTimeout)
	}
	if q.projection != nil {
		opts.SetProjection(q.projection)
	}
	if q.returnKey != nil {
		opts.SetReturnKey(*q.returnKey)
	}
	if q.showRecordId != nil {
		opts.SetShowRecordID(*q.showRecordId)
	}
	if q.skip != nil {
		opts.SetSkip(*q.skip)
	}
	if q.sort != nil {
		opts.SetSort(q.sort)
	}

	var cur, err = q.collection.Collection().Find(q.ctx, q.filter, opts)
	return &cursor{Cursor: cur, err: err}
}

type FindUpdate interface {
	ArrayFilters(filters ArrayFilters) FindUpdate

	BypassDocumentValidation(b bool) FindUpdate

	Collation(c *Collation) FindUpdate

	MaxTime(d time.Duration) FindUpdate

	Project(projection interface{}) FindUpdate

	ReturnDocument(rd ReturnDocument) FindUpdate

	Sort(fields ...string) FindUpdate

	Upsert(b bool) FindUpdate

	Hint(hint interface{}) FindUpdate

	Apply(result interface{}) error
}

type findUpdate struct {
	filter interface{}
	update interface{}

	ctx        context.Context
	opts       *options.FindOneAndUpdateOptions
	collection Collection
}

func (this *findUpdate) ArrayFilters(filters ArrayFilters) FindUpdate {
	this.opts.SetArrayFilters(filters)
	return this
}

func (this *findUpdate) BypassDocumentValidation(b bool) FindUpdate {
	this.opts.SetBypassDocumentValidation(b)
	return this
}

func (this *findUpdate) Collation(c *Collation) FindUpdate {
	this.opts.SetCollation(c)
	return this
}

func (this *findUpdate) MaxTime(d time.Duration) FindUpdate {
	this.opts.SetMaxTime(d)
	return this
}

func (this *findUpdate) Project(projection interface{}) FindUpdate {
	this.opts.SetProjection(projection)
	return this
}

func (this *findUpdate) ReturnDocument(rd ReturnDocument) FindUpdate {
	this.opts.SetReturnDocument(rd)
	return this
}

func (this *findUpdate) Sort(fields ...string) FindUpdate {
	if len(fields) == 0 {
		return this
	}
	this.opts.SetSort(sortFields(fields...))
	return this
}

func (this *findUpdate) Upsert(b bool) FindUpdate {
	this.opts.SetUpsert(b)
	return this
}

func (this *findUpdate) Hint(hint interface{}) FindUpdate {
	this.opts.SetHint(hint)
	return this
}

func (this *findUpdate) Apply(result interface{}) error {
	var err = this.collection.Collection().FindOneAndUpdate(this.ctx, this.filter, this.update, this.opts).Decode(result)
	return err
}

type FindReplace interface {
	BypassDocumentValidation(b bool) FindReplace

	Collation(c *Collation) FindReplace

	MaxTime(d time.Duration) FindReplace

	Project(projection interface{}) FindReplace

	ReturnDocument(rd ReturnDocument) FindReplace

	Sort(fields ...string) FindReplace

	Upsert(b bool) FindReplace

	Hint(hint interface{}) FindReplace

	Apply(result interface{}) error
}

type findReplace struct {
	filter      interface{}
	replacement interface{}

	ctx        context.Context
	opts       *options.FindOneAndReplaceOptions
	collection Collection
}

func (this *findReplace) BypassDocumentValidation(b bool) FindReplace {
	this.opts.SetBypassDocumentValidation(b)
	return this
}

func (this *findReplace) Collation(c *Collation) FindReplace {
	this.opts.SetCollation(c)
	return this
}

func (this *findReplace) MaxTime(d time.Duration) FindReplace {
	this.opts.SetMaxTime(d)
	return this
}

func (this *findReplace) Project(projection interface{}) FindReplace {
	this.opts.SetProjection(projection)
	return this
}

func (this *findReplace) ReturnDocument(rd ReturnDocument) FindReplace {
	this.opts.SetReturnDocument(rd)
	return this
}

func (this *findReplace) Sort(fields ...string) FindReplace {
	if len(fields) == 0 {
		return this
	}
	this.opts.SetSort(sortFields(fields...))
	return this
}

func (this *findReplace) Upsert(b bool) FindReplace {
	this.opts.SetUpsert(b)
	return this
}

func (this *findReplace) Hint(hint interface{}) FindReplace {
	this.opts.SetHint(hint)
	return this
}

func (this *findReplace) Apply(result interface{}) error {
	var err = this.collection.Collection().FindOneAndReplace(this.ctx, this.filter, this.replacement, this.opts).Decode(result)
	return err
}

type FindDelete interface {
	Collation(c *Collation) FindDelete

	MaxTime(d time.Duration) FindDelete

	Project(projection interface{}) FindDelete

	Sort(fields ...string) FindDelete

	Hint(hint interface{}) FindDelete

	Apply(result interface{}) error
}

type findDelete struct {
	filter interface{}

	ctx        context.Context
	opts       *options.FindOneAndDeleteOptions
	collection Collection
}

func (this *findDelete) Collation(c *Collation) FindDelete {
	this.opts.SetCollation(c)
	return this
}

func (this *findDelete) MaxTime(d time.Duration) FindDelete {
	this.opts.SetMaxTime(d)
	return this
}

func (this *findDelete) Project(projection interface{}) FindDelete {
	this.opts.SetProjection(projection)
	return this
}

func (this *findDelete) Sort(fields ...string) FindDelete {
	if len(fields) == 0 {
		return this
	}
	this.opts.SetSort(sortFields(fields...))
	return this
}

func (this *findDelete) Hint(hint interface{}) FindDelete {
	this.opts.SetHint(hint)
	return this
}

func (this *findDelete) Apply(result interface{}) error {
	var err = this.collection.Collection().FindOneAndDelete(this.ctx, this.filter, this.opts).Decode(result)
	return err
}
