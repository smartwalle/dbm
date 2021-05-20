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

func (this *query) Project(projection interface{}) Query {
	this.projection = projection
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
	this.showRecordId = &b
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
	this.sort = sortFields(fields...)
	return this
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
	if this.showRecordId != nil {
		opts.SetShowRecordID(*this.showRecordId)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}
	if this.sort != nil {
		opts.SetSort(this.sort)
	}

	return this.collection.Collection().FindOne(this.ctx, this.filter, opts).Decode(result)
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

	return this.collection.Collection().CountDocuments(this.ctx, this.filter, opts)
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
	if this.showRecordId != nil {
		opts.SetShowRecordID(*this.showRecordId)
	}
	if this.skip != nil {
		opts.SetSkip(*this.skip)
	}
	if this.sort != nil {
		opts.SetSort(this.sort)
	}

	var cur, err = this.collection.Collection().Find(this.ctx, this.filter, opts)
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
