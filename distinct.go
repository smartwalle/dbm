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

func (d *distinct) Collation(c *Collation) Distinct {
	d.opts.SetCollation(c)
	return d
}

func (d *distinct) MaxTime(duration time.Duration) Distinct {
	d.opts.SetMaxTime(duration)
	return d
}

func (d *distinct) Apply(result interface{}) error {
	var resultValue = reflect.ValueOf(result)
	if resultValue.Kind() != reflect.Ptr {
		return ErrResultNotSlice
	}

	var resultElemKind = resultValue.Elem().Kind()
	if resultElemKind != reflect.Interface && resultElemKind != reflect.Slice {
		return ErrResultNotSlice
	}

	var data, err = d.collection.Collection().Distinct(d.ctx, d.fieldName, d.filter, d.opts)
	if err != nil {
		return err
	}

	valueType, valueBytes, err := bson.MarshalValueWithRegistry(d.collection.Database().Client().Registry(), data)
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
