package initialize

import (
	"errors"
	"strings"
	"time"

	"go-app/pkg/setting"

	"github.com/spf13/viper"
)

func LoadConfig() (setting.Config, error) {
	v := viper.New()
	v.AddConfigPath("./config")
	v.SetConfigName("local")
	v.SetConfigType("yaml")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	v.SetDefault("server.address", ":8080")
	v.SetDefault("server.mode", "release")
	v.SetDefault("server.shutdown-timeout", 10*time.Second)
	v.SetDefault("mongo.uri", "mongodb://localhost:27018/")
	v.SetDefault("mongo.database", "chat_direct")
	v.SetDefault("auth.disabled", false)
	v.SetDefault("auth.default-user-id", "")
	v.SetDefault("cors.allow-origins", []string{"http://localhost:3000"})
	v.SetDefault("kafka.brokers", "localhost:9092")
	v.SetDefault("kafka.client-id", "chat-service")
	v.SetDefault("kafka.chat-events-topic", "chat.conversation.events.v1")
	v.SetDefault("kafka.ssl", false)
	v.SetDefault("kafka.outbox-poll-interval", time.Second)
	v.SetDefault("kafka.outbox-lease-duration", 30*time.Second)
	v.SetDefault("kafka.publish-timeout", 10*time.Second)

	keys := []string{
		"server.address", "server.mode", "server.shutdown-timeout",
		"mongo.uri", "mongo.database",
		"keycloak.issuer", "keycloak.audience", "keycloak.public-key", "keycloak.public-key-file",
		"auth.disabled", "auth.default-user-id",
		"cors.allow-origins",
		"kafka.brokers", "kafka.client-id", "kafka.chat-events-topic",
		"kafka.username", "kafka.password", "kafka.ssl",
		"kafka.outbox-poll-interval", "kafka.outbox-lease-duration", "kafka.publish-timeout",
	}
	for _, key := range keys {
		if err := v.BindEnv(key); err != nil {
			return setting.Config{}, err
		}
	}

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return setting.Config{}, err
		}
	}

	var cfg setting.Config
	if err := v.Unmarshal(&cfg); err != nil {
		return setting.Config{}, err
	}
	if cfg.Keycloak.Issuer == "" {
		return setting.Config{}, errors.New("KEYCLOAK_ISSUER is required")
	}
	if cfg.Keycloak.Audience == "" {
		return setting.Config{}, errors.New("KEYCLOAK_AUDIENCE is required")
	}
	if cfg.Server.ShutdownTimeout <= 0 || cfg.Mongo.URI == "" || cfg.Mongo.Database == "" {
		return setting.Config{}, errors.New("invalid server or Mongo configuration")
	}
	if len(cfg.CORS.AllowOrigins) == 0 {
		return setting.Config{}, errors.New("CORS allow-origins must not be empty")
	}
	for _, origin := range cfg.CORS.AllowOrigins {
		if strings.Contains(origin, "*") {
			return setting.Config{}, errors.New("CORS requires explicit origins")
		}
	}
	if cfg.Keycloak.PublicKey == "" && cfg.Keycloak.PublicKeyFile == "" {
		return setting.Config{}, errors.New("KEYCLOAK_PUBLIC_KEY or KEYCLOAK_PUBLIC_KEY_FILE is required")
	}
	if cfg.Auth.Disabled && strings.TrimSpace(cfg.Auth.DefaultUserID) == "" {
		return setting.Config{}, errors.New("AUTH_DEFAULT_USER_ID is required when authentication is disabled")
	}
	if strings.TrimSpace(cfg.Kafka.Brokers) == "" || strings.TrimSpace(cfg.Kafka.ClientID) == "" || strings.TrimSpace(cfg.Kafka.ChatEventsTopic) == "" {
		return setting.Config{}, errors.New("Kafka brokers, client ID, and chat events topic are required")
	}
	if (cfg.Kafka.Username == "") != (cfg.Kafka.Password == "") {
		return setting.Config{}, errors.New("set both KAFKA_USERNAME and KAFKA_PASSWORD, or leave both unset")
	}
	if cfg.Kafka.OutboxPollInterval <= 0 || cfg.Kafka.OutboxLeaseDuration <= 0 || cfg.Kafka.PublishTimeout <= 0 || cfg.Kafka.OutboxLeaseDuration <= cfg.Kafka.PublishTimeout {
		return setting.Config{}, errors.New("Kafka outbox intervals must be positive and lease duration must exceed publish timeout")
	}
	return cfg, nil
}
