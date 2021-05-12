package dbm

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"time"
)

type Distinct interface {
	Collation(c *Collation) Distinct

	MaxTime(d time.Duration) Distinct

	Apply(result interface{}) error
}

type distinct struct {
	filter     interface{}
	fieldName  string
	ctx        context.Context
	opts       *options.DistinctOptions
	collection Collection
}

func (this *distinct) Collation(c *Collation) Distinct {
	this.opts.SetCollation(c)
	return this
}

func (this *distinct) MaxTime(d time.Duration) Distinct {
	this.opts.SetMaxTime(d)
	return this
}

func (this *distinct) Apply(result interface{}) error {
	var resultValue = reflect.ValueOf(result)
	if resultValue.Kind() != reflect.Ptr {
		return ErrResultNotSlice
	}

	var resultElemKind = resultValue.Elem().Kind()
	if resultElemKind != reflect.Interface && resultElemKind != reflect.Slice {
		return ErrResultNotSlice
	}

	var data, err = this.collection.Collection().Distinct(this.ctx, this.fieldName, this.filter, this.opts)
	if err != nil {
		return err
	}

	valueType, valueBytes, err := bson.MarshalValueWithRegistry(this.collection.Database().Client().Registry(), data)
	if err != nil {
		return err
	}
	var rawValue = bson.RawValue{}
	rawValue.Type = valueType
	rawValue.Value = valueBytes
	if err = rawValue.Unmarshal(result); err != nil {
		return err
	}
	return nil
}
