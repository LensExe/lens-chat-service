package setting

import "time"

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Mongo    MongoConfig    `mapstructure:"mongo"`
	Keycloak KeycloakConfig `mapstructure:"keycloak"`
	Auth     AuthConfig     `mapstructure:"auth"`
	CORS     CORSConfig     `mapstructure:"cors"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
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

type AuthConfig struct {
	Disabled      bool   `mapstructure:"disabled"`
	DefaultUserID string `mapstructure:"default-user-id"`
}

type CORSConfig struct {
	AllowOrigins []string `mapstructure:"allow-origins"`
}

type KafkaConfig struct {
	Brokers             string        `mapstructure:"brokers"`
	ClientID            string        `mapstructure:"client-id"`
	ChatEventsTopic     string        `mapstructure:"chat-events-topic"`
	Username            string        `mapstructure:"username"`
	Password            string        `mapstructure:"password"`
	TLS                 bool          `mapstructure:"ssl"`
	OutboxPollInterval  time.Duration `mapstructure:"outbox-poll-interval"`
	OutboxLeaseDuration time.Duration `mapstructure:"outbox-lease-duration"`
	PublishTimeout      time.Duration `mapstructure:"publish-timeout"`
}
