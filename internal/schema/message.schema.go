package schema

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const CollectionMessages = "messages"

type Message struct {
	ID               primitive.ObjectID `bson:"_id,omitempty"`
	TenantID         string             `bson:"tenant_id"`
	ChannelID        primitive.ObjectID `bson:"channel_id"`
	SenderID         string             `bson:"sender_id"`
	ClientMessageID  string             `bson:"client_message_id"`
	RequestHash      string             `bson:"request_hash"`
	Type             string             `bson:"type"`
	Content          string             `bson:"content"`
	Sequence         int64              `bson:"seq"`
	ReplyToMessageID primitive.ObjectID `bson:"reply_to_message_id,omitempty"`
	Revision         int64              `bson:"revision"`
	RecalledAt       *time.Time         `bson:"recalled_at,omitempty"`
	CreatedAt        time.Time          `bson:"created_at"`
	UpdatedAt        time.Time          `bson:"updated_at"`
}
