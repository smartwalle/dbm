package dbm

import (
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

type D = primitive.D

type E = primitive.E

type M = primitive.M

type A = primitive.A

type DateTime = primitive.DateTime

type Timestamp = primitive.Timestamp

type Null = primitive.Null

type Regex = primitive.Regex

type Pipeline = mongo.Pipeline
