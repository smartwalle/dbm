package dbm

import "go.mongodb.org/mongo-driver/mongo/options"

type Config struct {
	*options.ClientOptions
}

func NewConfig(uri string) *Config {
	var cfg = &Config{}
	cfg.ClientOptions = options.Client()
	cfg.ClientOptions.ApplyURI(uri)
	return cfg
}
