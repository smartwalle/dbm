package dbm

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type ObjectId = primitive.ObjectID

func NewObjectId() ObjectId {
	return primitive.NewObjectIDFromTimestamp(time.Now())
}

func NewObjectIdFromTime(t time.Time) ObjectId {
	return primitive.NewObjectIDFromTimestamp(t)
}

func ObjectIdFromHex(s string) (ObjectId, error) {
	return primitive.ObjectIDFromHex(s)
}

func MustObjectId(s string) ObjectId {
	var id, err = primitive.ObjectIDFromHex(s)
	if err != nil {
		panic(err)
	}
	return id
}

func IsValidObjectId(s string) bool {
	return primitive.IsValidObjectID(s)
}

// E represents a BSON element for a D. It is usually used inside a D.
type E = bson.E

// D is an ordered representation of a BSON document.
type D = bson.D

// M is an unordered representation of a BSON document.
type M = bson.M

// An A is an ordered representation of a BSON array.
type A = bson.A

type Raw = bson.Raw

type DateTime = primitive.DateTime

type Timestamp = primitive.Timestamp

type Null = primitive.Null

type Regex = primitive.Regex

type Decimal128 = primitive.Decimal128

type Pipeline = mongo.Pipeline

func NewDecimal128(h, l uint64) Decimal128 {
	return primitive.NewDecimal128(h, l)
}

func NE(key string, value interface{}) E {
	return E{Key: key, Value: value}
}

func ND(items ...E) D {
	return D(items)
}

func NA(items ...interface{}) A {
	return A(items)
}

func NR(pattern, options string) Regex {
	return Regex{Pattern: pattern, Options: options}
}

func NP(items ...D) Pipeline {
	return Pipeline(items)
}
