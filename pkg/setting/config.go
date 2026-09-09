package setting

import "time"

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Mongo    MongoConfig    `mapstructure:"mongo"`
	Keycloak KeycloakConfig `mapstructure:"keycloak"`
	CORS     CORSConfig     `mapstructure:"cors"`
}

type ServerConfig struct {
	Address         string        `mapstructure:"address"`
	Mode            string        `mapstructure:"mode"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown-timeout"`
}

type MongoConfig struct {
	URI      string `mapstructure:"uri"`
	Database string `mapstructure:"database"`
}

type KeycloakConfig struct {
	Issuer        string `mapstructure:"issuer"`
	Audience      string `mapstructure:"audience"`
	PublicKey     string `mapstructure:"public-key"`
	PublicKeyFile string `mapstructure:"public-key-file"`
}

type CORSConfig struct {
	AllowOrigins []string `mapstructure:"allow-origins"`
}
