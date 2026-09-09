package schema

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const CollectionChannelStates = "channel_states"

type ChannelState struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	TenantID    string             `bson:"tenant_id"`
	ChannelID   primitive.ObjectID `bson:"channel_id"`
	UserID      string             `bson:"user_id"`
	LastReadSeq int64              `bson:"last_read_seq"`
	UpdatedAt   time.Time          `bson:"updated_at"`
}
