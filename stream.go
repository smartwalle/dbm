package dbm

import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ChangeStreamOptions = options.ChangeStreamOptions

func NewChangeStreamOptions() *ChangeStreamOptions {
	return options.ChangeStream()
}

type ChangeStream = mongo.ChangeStream

type OperationType string

const (
	OperationTypeInsert     = "insert"
	OperationTypeDelete     = "delete"
	OperationTypeReplace    = "replace"
	OperationTypeUpdate     = "update"
	OperationTypeInvalidate = "invalidate"
)

type ChangeEvent struct {
	Id            EventId       `bson:"_id"`
	OperationType OperationType `bson:"operationType"`
	ClusterTime   Timestamp     `bson:"clusterTime"`
	Namespace     Namespace     `bson:"ns"`
	WallTime      DateTime      `bson:"wallTime"`
	DocumentKey   DocumentKey   `bson:"documentKey"`
}

type EventId struct {
	Data string `bson:"_data"`
}

type Namespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

type DocumentKey struct {
	Id string `bson:"_id"`
}
