package schema

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const CollectionChannels = "channels"

type Channel struct {
	ID             primitive.ObjectID `bson:"_id,omitempty"`
	TenantID       string             `bson:"tenant_id"`
	DirectKey      string             `bson:"direct_key"`
	ParticipantIDs [2]string          `bson:"participant_ids"`
	LastMessageID  primitive.ObjectID `bson:"last_message_id,omitempty"`
	LastMessageSeq int64              `bson:"last_message_seq"`
	LastMessageAt  *time.Time         `bson:"last_message_at,omitempty"`
	Active         bool               `bson:"active"`
	CreatedAt      time.Time          `bson:"created_at"`
	UpdatedAt      time.Time          `bson:"updated_at"`
}
