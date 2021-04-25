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
	var cfg = dbm.NewConfig("mongodb://192.168.1.77:27017")

	var client, err = dbm.NewClient(context.Background(), cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	var db = client.Database("sm")
	var tUser = db.Collection("user")

	//fmt.Println(client.ServerVersion())
	//fmt.Println(client.TransactionAllowed())

	//var u1 = &User{Id: dbm.NewObjectId(), Name: "user1", Age: 10}
	//var u2 = &User{Id: dbm.NewObjectId(), Name: "user2", Age: 20}
	//var u3 = &User{Id: dbm.NewObjectId(), Name: "user3", Age: 30}
	//
	//user.Insert(context.Background(), u1, u2, u3)

	var u1 *User
	tUser.Find(context.Background(), dbm.M{"age": 20}).One(&u1)
	fmt.Println(u1)

	var uList []*User
	tUser.Find(context.Background(), dbm.M{}).All(&uList)
	fmt.Println(uList)

	var cursor = tUser.Find(context.Background(), dbm.M{}).Cursor()
	defer cursor.Close()
	for {
		ok := cursor.Next(context.Background())

		if ok == false {
			break
		}

		var u *User
		err = cursor.One(context.Background(), &u)

		fmt.Println(err, ok, u)
		if err != nil {
			break
		}
	}
}
