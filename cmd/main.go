package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/dbm"
	"time"
)

type User struct {
	Id   dbm.ObjectId `bson:"_id"`
	Name string       `bson:"name"`
	Age  int          `bson:"age"`
}

type Item struct {
	Id   dbm.ObjectId `bson:"_id"`
	Name string       `bson:"name"`
}

func main() {
	var cfg = dbm.NewConfig("mongodb+srv://smartwalle:smartwalle@smartwalle.kbxxd.mongodb.net/?retryWrites=true&w=majority")
	//var cfg = dbm.NewConfig("mongodb://192.168.1.77:30000")

	var client, err = dbm.NewClient(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close(context.Background())

	var db = client.Database("db")
	var tUser = db.Collection("user")

	var u = &User{}
	u.Id = dbm.MustObjectId("6087a3d22364dbb6cb13892f")
	u.Name = "xxx"
	u.Age = 10

	for i := 0; i < 100000; i++ {

		var u = &User{}
		u.Id = dbm.NewObjectId()
		tUser.InsertOne(context.Background(), u)
	}

	for {
		var uList []*User
		tUser.Find(context.Background(), dbm.M{}).Limit(1).All(&uList)
		fmt.Println(time.Now(), uList)

		//var u = &User{}
		//u.Id = dbm.NewObjectId()
		//tUser.InsertOne(context.Background(), u)

		time.Sleep(time.Second * 2)
	}

}
