package initialize

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"go-app/internal/channel"
	channelrepo "go-app/internal/channel/repo"
	"go-app/internal/message"
	messagerepo "go-app/internal/message/repo"
	"go-app/internal/middleware"
	"go-app/internal/router"
	"go-app/internal/websocket"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"net/http"
	"time"
)

func Run(ctx context.Context) error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	verifier, err := middleware.NewKeycloakVerifier(cfg.Keycloak)
	if err != nil {
		return err
	}
	switch cfg.Server.Mode {
	case gin.DebugMode, gin.ReleaseMode, gin.TestMode:
		gin.SetMode(cfg.Server.Mode)
	default:
		return errors.New("invalid server mode")
	}
	startup, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	client, err := mongo.Connect(startup, options.Client().ApplyURI(cfg.Mongo.URI).SetTimeout(10*time.Second))
	if err != nil {
		return err
	}
	defer func() {
		shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		client.Disconnect(shutdown)
	}()
	if err = client.Ping(startup, readpref.Primary()); err != nil {
		return err
	}
	db := client.Database(cfg.Mongo.Database)
	chrepo := channelrepo.New(db)
	msgrepo := messagerepo.New(db)
	if err = chrepo.EnsureIndexes(startup); err != nil {
		return err
	}
	if err = msgrepo.EnsureIndexes(startup); err != nil {
		return err
	}
	hub := websocket.NewHub()
	defer hub.Close()
	channels := channel.NewService(chrepo)
	messages := message.NewService(msgrepo, channels, hub)
	routes := router.New(channel.NewController(channels), message.NewController(messages), websocket.NewHandler(hub, cfg.CORS.AllowOrigins), verifier, cfg.CORS.AllowOrigins, func(ctx context.Context) error { return client.Ping(ctx, readpref.Primary()) })
	server := &http.Server{Addr: cfg.Server.Address, Handler: routes, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, c := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer c()
		hub.Close()
		err = server.Shutdown(shutdown)
		if err != nil {
			server.Close()
		}
		return err
	}
}
