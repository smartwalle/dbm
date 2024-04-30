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
	Default       = options.Default
	Off           = options.Off
	Required      = options.Required
	UpdateLookup  = options.UpdateLookup
	WhenAvailable = options.WhenAvailable
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

func (fu *findUpdate) ArrayFilters(filters ArrayFilters) FindUpdate {
	fu.opts.SetArrayFilters(filters)
	return fu
}

func (fu *findUpdate) BypassDocumentValidation(b bool) FindUpdate {
	fu.opts.SetBypassDocumentValidation(b)
	return fu
}

func (fu *findUpdate) Collation(c *Collation) FindUpdate {
	fu.opts.SetCollation(c)
	return fu
}

func (fu *findUpdate) MaxTime(d time.Duration) FindUpdate {
	fu.opts.SetMaxTime(d)
	return fu
}

func (fu *findUpdate) Project(projection interface{}) FindUpdate {
	fu.opts.SetProjection(projection)
	return fu
}

func (fu *findUpdate) ReturnDocument(rd ReturnDocument) FindUpdate {
	fu.opts.SetReturnDocument(rd)
	return fu
}

func (fu *findUpdate) Sort(fields ...string) FindUpdate {
	if len(fields) == 0 {
		return fu
	}
	fu.opts.SetSort(sortFields(fields...))
	return fu
}

func (fu *findUpdate) Upsert(b bool) FindUpdate {
	fu.opts.SetUpsert(b)
	return fu
}

func (fu *findUpdate) Hint(hint interface{}) FindUpdate {
	fu.opts.SetHint(hint)
	return fu
}

func (fu *findUpdate) Apply(result interface{}) error {
	var err = fu.collection.Collection().FindOneAndUpdate(fu.ctx, fu.filter, fu.update, fu.opts).Decode(result)
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

func (fr *findReplace) BypassDocumentValidation(b bool) FindReplace {
	fr.opts.SetBypassDocumentValidation(b)
	return fr
}

func (fr *findReplace) Collation(c *Collation) FindReplace {
	fr.opts.SetCollation(c)
	return fr
}

func (fr *findReplace) MaxTime(d time.Duration) FindReplace {
	fr.opts.SetMaxTime(d)
	return fr
}

func (fr *findReplace) Project(projection interface{}) FindReplace {
	fr.opts.SetProjection(projection)
	return fr
}

func (fr *findReplace) ReturnDocument(rd ReturnDocument) FindReplace {
	fr.opts.SetReturnDocument(rd)
	return fr
}

func (fr *findReplace) Sort(fields ...string) FindReplace {
	if len(fields) == 0 {
		return fr
	}
	fr.opts.SetSort(sortFields(fields...))
	return fr
}

func (fr *findReplace) Upsert(b bool) FindReplace {
	fr.opts.SetUpsert(b)
	return fr
}

func (fr *findReplace) Hint(hint interface{}) FindReplace {
	fr.opts.SetHint(hint)
	return fr
}

func (fr *findReplace) Apply(result interface{}) error {
	var err = fr.collection.Collection().FindOneAndReplace(fr.ctx, fr.filter, fr.replacement, fr.opts).Decode(result)
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

func (fd *findDelete) Collation(c *Collation) FindDelete {
	fd.opts.SetCollation(c)
	return fd
}

func (fd *findDelete) MaxTime(d time.Duration) FindDelete {
	fd.opts.SetMaxTime(d)
	return fd
}

func (fd *findDelete) Project(projection interface{}) FindDelete {
	fd.opts.SetProjection(projection)
	return fd
}

func (fd *findDelete) Sort(fields ...string) FindDelete {
	if len(fields) == 0 {
		return fd
	}
	fd.opts.SetSort(sortFields(fields...))
	return fd
}

func (fd *findDelete) Hint(hint interface{}) FindDelete {
	fd.opts.SetHint(hint)
	return fd
}

func (fd *findDelete) Apply(result interface{}) error {
	var err = fd.collection.Collection().FindOneAndDelete(fd.ctx, fd.filter, fd.opts).Decode(result)
	return err
}
