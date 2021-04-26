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

	var db = client.Database("db")
	var tUser = db.Collection("user")

	var uList []*User
	tUser.Find(context.Background(), dbm.M{}).All(&uList)
	fmt.Println(uList)
}
