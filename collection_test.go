package dbm_test

import (
	"context"
	"github.com/smartwalle/dbm"
	"go.mongodb.org/mongo-driver/mongo/options"
	"testing"
)

type UserChangeEvent struct {
	dbm.ChangeEvent `bson:",inline"`
	FullDocument    User `bson:"fullDocument"`
}

func TestCollection_Watch(t *testing.T) {
	var db = getDatabase(t)
	defer db.Client().Close(context.Background())
	var tUser = db.Collection("user")

	var pipe = dbm.NP()
	var opts = options.ChangeStream()
	opts.SetFullDocument(options.UpdateLookup)

	var stream, err = tUser.Watch(context.Background(), pipe, opts)
	if err != nil {
		t.Fatal("Watch Error", err)
	}
	defer stream.Close(context.Background())

	for stream.Next(context.Background()) {
		var uEvent *UserChangeEvent
		stream.Decode(&uEvent)

		t.Log(uEvent.OperationType, uEvent.DocumentKey.Id, uEvent.FullDocument)
	}
}
