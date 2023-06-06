package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/dbm"
)

type Base struct {
	Id       dbm.ObjectId `bson:"_id"`
	ServerId int          `bson:"server_id"`
	UserId   int64        `bson:"user_id"`
}

type User struct {
	Base   `bson:",inline"`
	Name   string `bson:"name"`
	Age    int    `bson:"age"`
	Gender int    `bson:"gender"`
	Point  int    `bson:"point"`
}

func main() {
	var cfg = dbm.NewConfig("mongodb+srv://smartwalle:smartwalle@smartwalle.kbxxd.mongodb.net/?retryWrites=true&w=majority")

	var client, err = dbm.New(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close(context.Background())

	var db = client.Database("test")
	var tUser = db.Collection("user")

	var u1 = &User{}
	u1.Id = dbm.NewObjectId()
	u1.Age = 11

	fmt.Println(tUser.InsertOne(context.Background(), u1))

	return

	//tUser.Drop(context.Background())
	//
	//var indexView = tUser.IndexView()
	//indexView.Drop(context.Background(), "us")
	//indexView.CreateIndex(context.Background(), "us", []string{"server_id", "user_id"})
	//
	//for i := 0; i < 100; i++ {
	//	var u = &User{}
	//	u.ServerId = i % 10
	//	u.UserId = xid.Next()
	//	u.Name = "u" + strconv.Itoa(i)
	//	u.Age = i + 1
	//	u.Gender = i%2 + 1
	//	u.Point = (i + 1) * 10
	//
	//	tUser.InsertOne(context.Background(), u)
	//}

	//var filter = []dbm.M{{"$match": dbm.M{"server_id": 1}}, {"$sample": dbm.M{"size": 1}}}

	var match = dbm.D{
		{"server_id", 1},
	}
	var sample = dbm.D{
		{"size", 1},
	}

	var pipe = []dbm.D{
		{
			{"$match", match},
		},
		{
			{"$sample", sample},
		},
	}

	var results []*User
	err = tUser.Aggregate(context.Background(), pipe).All(&results)
	if err != nil {
		fmt.Println(err)
	}

	for _, u := range results {
		fmt.Println(u.Id, u.ServerId, u.UserId, u.Name, u.Gender, u.Age, u.Point)
	}
}
