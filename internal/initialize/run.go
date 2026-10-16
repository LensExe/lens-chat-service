package initialize

import (
	"context"
	"errors"
	"go-app/internal/channel"
	channelrepo "go-app/internal/channel/repo"
	"go-app/internal/message"
	messagerepo "go-app/internal/message/repo"
	"go-app/internal/middleware"
	"go-app/internal/outbox"
	"go-app/internal/router"
	"go-app/pkg/setting"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type repositories struct {
	channels *channelrepo.Repository
	messages *messagerepo.Repository
	outbox   *outbox.Repository
}

type mongoRuntime struct {
	client  *mongo.Client
	startup context.Context
	cancel  context.CancelFunc
}

type relayRuntime struct {
	publisher *outbox.KafkaPublisher
	telemetry *outbox.Telemetry
	stop      context.CancelFunc
	done      chan struct{}
}

func Run(ctx context.Context) error {
	cfg, verifier, err := loadRuntimeConfig()
	if err != nil {
		return err
	}
	if cfg.Auth.Disabled {
		slog.Warn(
			"authentication middleware is disabled",
			"scope", "chat API routes",
			"environment", "local-only",
		)
	}

	mongoConn, err := connectMongo(ctx, cfg.Mongo)
	if err != nil {
		return err
	}
	defer mongoConn.Close()

	repos, err := initializeRepositories(
		mongoConn.startup,
		mongoConn.client.Database(cfg.Mongo.Database),
	)
	if err != nil {
		return err
	}

	relay, err := startOutboxRelay(ctx, cfg.Kafka, repos.outbox)
	if err != nil {
		return err
	}
	defer relay.Close()

	server := newHTTPServer(cfg, verifier, repos, mongoConn.client, relay.telemetry)
	slog.Info(
		"Chat service initialized",
		"address", cfg.Server.Address,
		"integrations", []string{"mongodb", "keycloak", "kafka"},
	)
	return serveHTTP(ctx, server, cfg.Server.ShutdownTimeout)
}

func loadRuntimeConfig() (setting.Config, *middleware.KeycloakVerifier, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return setting.Config{}, nil, err
	}

	verifier, err := middleware.NewKeycloakVerifier(cfg.Keycloak)
	if err != nil {
		return setting.Config{}, nil, err
	}
	if err := setGinMode(cfg.Server.Mode); err != nil {
		return setting.Config{}, nil, err
	}
	slog.Info(
		"Keycloak verifier initialized",
		"service", "keycloak",
		"issuer", cfg.Keycloak.Issuer,
		"audience", cfg.Keycloak.Audience,
		"algorithm", "RS256",
	)
	return cfg, verifier, nil
}

func setGinMode(mode string) error {
	switch mode {
	case gin.DebugMode, gin.ReleaseMode, gin.TestMode:
		gin.SetMode(mode)
		return nil
	default:
		return errors.New("invalid server mode")
	}
}

func connectMongo(parent context.Context, cfg setting.MongoConfig) (*mongoRuntime, error) {
	startup, cancel := context.WithTimeout(parent, 20*time.Second)
	client, err := mongo.Connect(startup, options.Client().ApplyURI(cfg.URI).SetTimeout(10*time.Second))
	if err != nil {
		cancel()
		return nil, err
	}

	runtime := &mongoRuntime{client: client, startup: startup, cancel: cancel}
	if err := client.Ping(startup, readpref.Primary()); err != nil {
		runtime.Close()
		return nil, err
	}
	slog.Info("MongoDB connected", "service", "mongodb", "database", cfg.Database)
	return runtime, nil
}

func (m *mongoRuntime) Close() {
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = m.client.Disconnect(shutdown)
	m.cancel()
}

func initializeRepositories(ctx context.Context, db *mongo.Database) (*repositories, error) {
	repos := &repositories{
		channels: channelrepo.New(db),
		messages: messagerepo.New(db),
		outbox:   outbox.NewRepository(db),
	}
	if err := repos.channels.EnsureIndexes(ctx); err != nil {
		return nil, err
	}
	if err := repos.messages.EnsureIndexes(ctx); err != nil {
		return nil, err
	}
	if err := repos.outbox.EnsureIndexes(ctx); err != nil {
		return nil, err
	}
	return repos, nil
}

func startOutboxRelay(
	ctx context.Context,
	cfg setting.KafkaConfig,
	outboxRepo *outbox.Repository,
) (*relayRuntime, error) {
	publisher, err := outbox.NewKafkaPublisher(cfg)
	if err != nil {
		return nil, err
	}
	slog.Info(
		"Kafka publisher initialized",
		"service", "kafka",
		"brokers", cfg.Brokers,
		"topic", cfg.ChatEventsTopic,
		"tls", cfg.TLS,
		"sasl", cfg.Username != "",
	)

	telemetry := outbox.NewTelemetry() //
	relay := outbox.NewRelay(
		outboxRepo,
		publisher,
		cfg.OutboxPollInterval,
		cfg.OutboxLeaseDuration,
		cfg.PublishTimeout,
		telemetry,
	) //
	relayCtx, stop := context.WithCancel(ctx)
	runtime := &relayRuntime{
		publisher: publisher,
		telemetry: telemetry,
		stop:      stop,
		done:      make(chan struct{}),
	}
	go func() {
		defer close(runtime.done)
		relay.Run(relayCtx)
	}()
	return runtime, nil
}

func (r *relayRuntime) Close() {
	r.stop()
	<-r.done // chờ goroutine relay hoàn thành
	if err := r.publisher.Close(); err != nil {
		slog.Warn("close Kafka publisher failed", "error", err)
	}
}

func newHTTPServer(
	cfg setting.Config,
	verifier *middleware.KeycloakVerifier,
	repos *repositories,
	client *mongo.Client,
	telemetry *outbox.Telemetry,
) *http.Server {
	channels := channel.NewService(repos.channels)
	messages := message.NewService(repos.messages, channels)
	routes := router.New(
		channel.NewController(channels),
		message.NewController(messages),
		verifier,
		cfg.Auth.Disabled,
		cfg.Auth.DefaultUserID,
		cfg.CORS.AllowOrigins,
		cfg.AIWorkerToken,
		func(ctx context.Context) error {
			return client.Ping(ctx, readpref.Primary())
		},
		func(ctx context.Context) (string, error) {
			return repos.outbox.RenderMetrics(ctx, telemetry)
		},
	)
	return &http.Server{
		Addr:              cfg.Server.Address,
		Handler:           routes,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func serveHTTP(
	ctx context.Context,
	server *http.Server,
	shutdownTimeout time.Duration,
) error {
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()

	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			server.Close()
			return err
		}
		return nil
	}
}
