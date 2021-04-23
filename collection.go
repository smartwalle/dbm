package dbm

import "go.mongodb.org/mongo-driver/mongo"

type Collection struct {
	*mongo.Collection
	database *Database
}

func (this *Collection) Database() *Database {
	return this.database
}
