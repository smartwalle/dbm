package dbm_test

import (
	"context"
	"github.com/smartwalle/dbm"
	"testing"
)

const (
	kTestURI      = "mongodb+srv://smartwalle:kVeZvFiOwDhnuAco@smartwalle.endqace.mongodb.net/?retryWrites=true&w=majority"
	kTestDatabase = "test"
)

type User struct {
	Id   string `bson:"_id"`
	Age  int    `bson:"age"`
	Name string `bson:"name"`
}

var cfg = dbm.NewConfig(kTestURI)

func getDatabase(t *testing.T) dbm.Database {
	var client, err = dbm.New(context.Background(), cfg)
	if err != nil {
		t.Fatal("连接数据库发生错误", err)
	}

	var db = client.Database(kTestDatabase)
	return db
}

func TestDatabase_BeginTxCommit(t *testing.T) {
	var db = getDatabase(t)
	defer db.Client().Close(context.Background())
	var tUser = db.Collection("user")

	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal("开启事务发生错误", err)
	}

	var uid1 = dbm.NewObjectId().Hex()
	var uid2 = dbm.NewObjectId().Hex()

	if _, err = tUser.InsertOne(tx, &User{Id: uid1, Age: 10, Name: "BeginCommit1-Good"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if _, err = tUser.InsertOne(tx, &User{Id: uid2, Age: 11, Name: "BeginCommit2-Good"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal("提交事务发生错误", err)
	}

	var nUsers []*User
	if err = tUser.Find(context.Background(), dbm.M{"_id": dbm.M{"$in": []string{uid1, uid2}}}).Sort("_id").All(&nUsers); err != nil {
		t.Fatal("查询数据发生错误", err)
	}

	if len(nUsers) != 2 {
		t.Fatal("没有查询到刚插入的数据")
	}

	if nUsers[0].Id != uid1 {
		t.Fatal("数据不匹配")
	}

	if nUsers[1].Id != uid2 {
		t.Fatal("数据不匹配")
	}
}

func TestDatabase_BeginTxRollback(t *testing.T) {
	var db = getDatabase(t)
	defer db.Client().Close(context.Background())
	var tUser = db.Collection("user")

	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal("开启事务发生错误", err)
	}

	var uid1 = dbm.NewObjectId().Hex()
	var uid2 = dbm.NewObjectId().Hex()

	if _, err = tUser.InsertOne(tx, &User{Id: uid1, Age: 10, Name: "BeginRollback1-Bad"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if _, err = tUser.InsertOne(tx, &User{Id: uid2, Age: 11, Name: "BeginRollback2-Bad"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if err = tx.Rollback(context.Background()); err != nil {
		t.Fatal("回滚事务发生错误", err)
	}

	var nUsers []*User
	if err = tUser.Find(context.Background(), dbm.M{"_id": dbm.M{"$in": []string{uid1, uid2}}}).Sort("_id").All(&nUsers); err != nil {
		t.Fatal("查询数据发生错误", err)
	}

	if len(nUsers) > 0 {
		t.Fatal("回滚失败")
	}
}

func TestDatabase_SessionCommit(t *testing.T) {
	var db = getDatabase(t)
	defer db.Client().Close(context.Background())
	var tUser = db.Collection("user")

	sess, err := db.StartSession()
	if err != nil {
		t.Fatal("StartSession 发生错误", err)
	}
	defer sess.EndSession(context.Background())

	tx, err := sess.BeginTx(context.Background())
	if err != nil {
		t.Fatal("开启事务发生错误", err)
	}

	var uid1 = dbm.NewObjectId().Hex()
	var uid2 = dbm.NewObjectId().Hex()

	if _, err = tUser.InsertOne(tx, &User{Id: uid1, Age: 10, Name: "SessionCommit1-Good"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if _, err = tUser.InsertOne(tx, &User{Id: uid2, Age: 11, Name: "SessionCommit2-Good"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal("提交事务发生错误", err)
	}

	var nUsers []*User
	if err = tUser.Find(context.Background(), dbm.M{"_id": dbm.M{"$in": []string{uid1, uid2}}}).Sort("_id").All(&nUsers); err != nil {
		t.Fatal("查询数据发生错误", err)
	}

	if len(nUsers) != 2 {
		t.Fatal("没有查询到刚插入的数据")
	}

	if nUsers[0].Id != uid1 {
		t.Fatal("数据不匹配")
	}

	if nUsers[1].Id != uid2 {
		t.Fatal("数据不匹配")
	}
}

func TestDatabase_SessionRollback(t *testing.T) {
	var db = getDatabase(t)
	defer db.Client().Close(context.Background())
	var tUser = db.Collection("user")

	sess, err := db.StartSession()
	if err != nil {
		t.Fatal("StartSession 发生错误", err)
	}
	defer sess.EndSession(context.Background())

	tx, err := sess.BeginTx(context.Background())
	if err != nil {
		t.Fatal("开启事务发生错误", err)
	}

	var uid1 = dbm.NewObjectId().Hex()
	var uid2 = dbm.NewObjectId().Hex()

	if _, err = tUser.InsertOne(tx, &User{Id: uid1, Age: 10, Name: "SessionRollback1-Bad"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if _, err = tUser.InsertOne(tx, &User{Id: uid2, Age: 11, Name: "SessionRollback2-Bad"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if err = tx.Rollback(context.Background()); err != nil {
		t.Fatal("回滚事务发生错误", err)
	}

	var nUsers []*User
	if err = tUser.Find(context.Background(), dbm.M{"_id": dbm.M{"$in": []string{uid1, uid2}}}).Sort("_id").All(&nUsers); err != nil {
		t.Fatal("查询数据发生错误", err)
	}

	if len(nUsers) > 0 {
		t.Fatal("回滚失败")
	}
}

func TestDatabase_EndSession(t *testing.T) {
	var db = getDatabase(t)
	defer db.Client().Close(context.Background())
	var tUser = db.Collection("user")

	sess, err := db.StartSession()
	if err != nil {
		t.Fatal("StartSession 发生错误", err)
	}

	tx, err := sess.BeginTx(context.Background())
	if err != nil {
		t.Fatal("开启事务发生错误", err)
	}

	var uid1 = dbm.NewObjectId().Hex()
	var uid2 = dbm.NewObjectId().Hex()

	if _, err = tUser.InsertOne(tx, &User{Id: uid1, Age: 10, Name: "EndSession1-Good"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if _, err = tUser.InsertOne(tx, &User{Id: uid2, Age: 11, Name: "EndSession2-Good"}); err != nil {
		tx.Rollback(context.Background())
		t.Fatal("插入数据发生错误", err)
	}

	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal("提交事务发生错误", err)
	}

	var nUsers []*User
	if err = tUser.Find(context.Background(), dbm.M{"_id": dbm.M{"$in": []string{uid1, uid2}}}).Sort("_id").All(&nUsers); err != nil {
		t.Fatal("查询数据发生错误", err)
	}

	if len(nUsers) != 2 {
		t.Fatal("没有查询到刚插入的数据")
	}

	if nUsers[0].Id != uid1 {
		t.Fatal("数据不匹配")
	}

	if nUsers[1].Id != uid2 {
		t.Fatal("数据不匹配")
	}

	sess.EndSession(context.Background())

	tx2, err := sess.BeginTx(context.Background())
	if err != nil {
		t.Fatal("开启事务发生错误", err)
	}

	if _, err = tUser.InsertOne(tx2, &User{Id: dbm.NewObjectId().String(), Age: 10, Name: "EndSession3-Bad"}); err == nil {
		t.Fatal("Session 已经关闭，这里应该报错")
	}
}
