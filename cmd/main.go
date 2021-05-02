package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/dbm"
)

type User struct {
	Id   dbm.ObjectId `bson:"_id"`
	Name string       `bson:"name"`
	Age  int          `bson:"age"`
}

func main() {
	var cfg = dbm.NewConfig("mongodb+srv://smartwalle:smartwalle@smartwalle.kbxxd.mongodb.net/?retryWrites=true&w=majority")

	var client, err = dbm.NewClient(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close(context.Background())

	var db = client.Database("test")
	var tUser = db.Collection("user")

	tUser.Drop(context.Background())

	// insert
	var u1 = &User{}
	u1.Id = dbm.NewObjectId()
	u1.Name = "test name"
	u1.Age = 18
	if _, err = tUser.InsertOne(context.Background(), u1); err != nil {
		fmt.Println("insert error:", err)
		return
	}
}
