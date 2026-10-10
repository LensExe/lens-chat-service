package outbox

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"go-app/internal/schema"
	"go-app/pkg/setting"
)

type Publisher interface {
	Publish(context.Context, string, schema.ChatEventEnvelope) error
	Close() error
}

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(cfg setting.KafkaConfig) (*KafkaPublisher, error) {
	brokers := make([]string, 0)
	for _, broker := range strings.Split(cfg.Brokers, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	if len(brokers) == 0 {
		return nil, errors.New("KAFKA_BROKERS must contain at least one broker")
	}
	if strings.TrimSpace(cfg.ChatEventsTopic) == "" {
		return nil, errors.New("KAFKA_CHAT_EVENTS_TOPIC must not be empty")
	}
	if (cfg.Username == "") != (cfg.Password == "") {
		return nil, errors.New("set both KAFKA_USERNAME and KAFKA_PASSWORD, or leave both unset")
	}

	var tlsConfig *tls.Config
	if cfg.TLS {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	var saslMechanism sasl.Mechanism
	if cfg.Username != "" {
		saslMechanism = plain.Mechanism{Username: cfg.Username, Password: cfg.Password}
	}

	return &KafkaPublisher{writer: &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  cfg.ChatEventsTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		MaxAttempts:            1,
		AllowAutoTopicCreation: false,
		Transport: &kafka.Transport{
			ClientID: cfg.ClientID,
			TLS:      tlsConfig,
			SASL:     saslMechanism,
		},
	}}, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, key string, event schema.ChatEventEnvelope) error {
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  event.OccurredAt,
	})
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
