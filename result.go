package dbm

import "go.mongodb.org/mongo-driver/mongo"

type InsertOneResult = mongo.InsertOneResult

type InsertManyResult = mongo.InsertManyResult

type DeleteResult = mongo.DeleteResult

type UpdateResult = mongo.UpdateResult

type BulkResult = mongo.BulkWriteResult
