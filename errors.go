package dbm

import (
	"errors"
	"go.mongodb.org/mongo-driver/mongo"
)

var ErrNoDocuments = mongo.ErrNoDocuments

var ErrSessionNotSupported = errors.New("session not supported")

var ErrResultNotSlice = errors.New("results argument must be a pointer to a slice")
