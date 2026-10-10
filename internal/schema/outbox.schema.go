package schema

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	CollectionOutboxEvents      = "outbox_events"
	CollectionOutboxRelayLeases = "outbox_relay_leases"

	OutboxStatusPending   = "pending"
	OutboxStatusPublished = "published"
	OutboxStatusFailed    = "failed"

	EventChatMessageCreated     = "chat.message.created.v1"
	EventChatMessageUpdated     = "chat.message.updated.v1"
	EventChatMessageRecalled    = "chat.message.recalled.v1"
	EventChatReadCursorAdvanced = "chat.read-cursor.advanced.v1"
)

// ChatEventEnvelope is the versioned contract published to Kafka.
type ChatEventEnvelope struct {
	EventID          string        `json:"event_id" bson:"event_id"`
	EventType        string        `json:"event_type" bson:"event_type"`
	SchemaVersion    int           `json:"schema_version" bson:"schema_version"`
	OccurredAt       time.Time     `json:"occurred_at" bson:"occurred_at"`
	Producer         string        `json:"producer" bson:"producer"`
	AggregateType    string        `json:"aggregate_type" bson:"aggregate_type"`
	AggregateID      string        `json:"aggregate_id" bson:"aggregate_id"`
	AggregateVersion int64         `json:"aggregate_version" bson:"aggregate_version"`
	Data             ChatEventData `json:"data" bson:"data"`
}

type ChatEventData struct {
	ChannelID      string                   `json:"channel_id" bson:"channel_id"`
	ParticipantIDs [2]string                `json:"participant_ids" bson:"participant_ids"`
	Message        *ChatMessageEventPayload `json:"message,omitempty" bson:"message,omitempty"`
	UserID         string                   `json:"user_id,omitempty" bson:"user_id,omitempty"`
	LastReadSeq    *int64                   `json:"last_read_seq,omitempty" bson:"last_read_seq,omitempty"`
}

type ChatMessageEventPayload struct {
	ID               string     `json:"id" bson:"id"`
	ChannelID        string     `json:"channel_id" bson:"channel_id"`
	SenderID         string     `json:"sender_id" bson:"sender_id"`
	ClientMessageID  string     `json:"client_message_id" bson:"client_message_id"`
	Type             string     `json:"type" bson:"type"`
	Content          string     `json:"content" bson:"content"`
	Sequence         int64      `json:"seq" bson:"seq"`
	ReplyToMessageID string     `json:"reply_to_message_id,omitempty" bson:"reply_to_message_id,omitempty"`
	Revision         int64      `json:"revision" bson:"revision"`
	RecalledAt       *time.Time `json:"recalled_at,omitempty" bson:"recalled_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at" bson:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at" bson:"updated_at"`
}

// OutboxEvent stores the Kafka envelope beside delivery state in MongoDB.
type OutboxEvent struct {
	ID               primitive.ObjectID `bson:"_id,omitempty"`
	EventID          string             `bson:"event_id"`
	Event            ChatEventEnvelope  `bson:"event"`
	PartitionKey     string             `bson:"partition_key"`
	AggregateVersion int64              `bson:"aggregate_version"`
	Status           string             `bson:"status"`
	AttemptCount     int64              `bson:"attempt_count"`
	NextAttemptAt    time.Time          `bson:"next_attempt_at"`
	PublishedAt      *time.Time         `bson:"published_at,omitempty"`
	FailedAt         *time.Time         `bson:"failed_at,omitempty"`
	LastError        string             `bson:"last_error,omitempty"`
	CreatedAt        time.Time          `bson:"created_at"`
}

type OutboxRelayLease struct {
	ID         string    `bson:"_id"`
	Owner      string    `bson:"owner"`
	LeaseUntil time.Time `bson:"lease_until"`
	UpdatedAt  time.Time `bson:"updated_at"`
}
