package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/dbm"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	var cfg = dbm.NewConfig("mongodb://192.168.1.77:27017")

	var client, err = dbm.NewClient(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close(context.Background())

	var db = client.Database("sm")
	var c = db.Collection("haha")

	fmt.Println(client.ServerVersion())
	fmt.Println(client.TransactionAllowed())
	return

	c.InsertOne(context.Background(), bson.M{"_id": dbm.NewObjectId(), "ss": 1, "ee": "22", "ess": dbm.NewObjectId()})
}