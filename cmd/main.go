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
	//var cfg = dbm.NewConfig("mongodb+srv://smartwalle:smartwalle@smartwalle.kbxxd.mongodb.net/?retryWrites=true&w=majority")
	var cfg = dbm.NewConfig("mongodb://192.168.1.77:30000")

	var client, err = dbm.NewClient(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close(context.Background())

	var db = client.Database("test")
	var tUser = db.Collection("user")

	for i := 0; i < 100000; i++ {
		var u = &User{}
		u.Id = dbm.NewObjectId()
		tUser.InsertOne(context.Background(), u)
	}
}
