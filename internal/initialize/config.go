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
	v.SetDefault("mongo.uri", "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true")
	v.SetDefault("mongo.database", "chat_direct")
	v.SetDefault("cors.allow-origins", []string{"http://localhost:3000"})

	keys := []string{
		"server.address", "server.mode", "server.shutdown-timeout",
		"mongo.uri", "mongo.database",
		"keycloak.issuer", "keycloak.audience", "keycloak.public-key", "keycloak.public-key-file",
		"cors.allow-origins",
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
	return cfg, nil
}
