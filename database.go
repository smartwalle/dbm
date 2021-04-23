package dbm

import "go.mongodb.org/mongo-driver/mongo"

type Database struct {
	*mongo.Database
	client *Client
}

func (this *Database) Client() *Client {
	return this.client
}

func (this *Database) Collection(name string) *Collection {
	return &Collection{Collection: this.Database.Collection(name), database: this}
}
