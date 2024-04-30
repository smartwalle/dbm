package main

import (
	"context"
	"github.com/smartwalle/dbm"
	"log/slog"
)

type User struct {
	Id   string `bson:"_id"`
	Age  int    `bson:"age"`
	Name string `bson:"name"`
}

type UserChangeEvent struct {
	dbm.ChangeEvent `bson:",inline"`
	FullDocument    User `bson:"fullDocument"`
}

func main() {
	var cfg = dbm.NewConfig("mongodb+srv://smartwalle:kVeZvFiOwDhnuAco@smartwalle.endqace.mongodb.net/?retryWrites=true&w=majority")

	var client, err = dbm.New(context.Background(), cfg)
	if err != nil {
		slog.Error("Connect database error", slog.Any("error", err))
		return
	}
	defer client.Close(context.Background())

	var db = client.Database("test")
	var tUser = db.Collection("user")

	var pipe = dbm.NP()
	var opts = dbm.NewChangeStreamOptions()
	opts.SetFullDocument(dbm.UpdateLookup)

	stream, err := tUser.Watch(context.Background(), pipe, opts)
	if err != nil {
		slog.Error("Watch error", slog.Any("error", err))
		return
	}
	defer stream.Close(context.Background())

	slog.Info("Running...")
	for stream.Next(context.Background()) {
		var uEvent *UserChangeEvent
		if err = stream.Decode(&uEvent); err != nil {
			slog.Error("Decode error", slog.Any("error", err))
			continue
		}
		slog.Info("UserChangeEvent", slog.Any("id", uEvent.DocumentKey.Id))
	}
}
