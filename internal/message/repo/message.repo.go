package repo

import (
	"context"
	"go-app/internal/schema"
	"go-app/pkg/response"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"time"
)

type Repository struct {
	db                         *mongo.Database
	messages, channels, states *mongo.Collection
}

func New(db *mongo.Database) *Repository {
	return &Repository{db: db, messages: db.Collection(schema.CollectionMessages), channels: db.Collection(schema.CollectionChannels), states: db.Collection(schema.CollectionChannelStates)}
}
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.messages.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "channel_id", Value: 1}, {Key: "seq", Value: -1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "channel_id", Value: 1}, {Key: "sender_id", Value: 1}, {Key: "client_message_id", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	if err != nil {
		return err
	}
	_, err = r.states.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "channel_id", Value: 1}, {Key: "user_id", Value: 1}}, Options: options.Index().SetUnique(true)})
	return err
}
func (r *Repository) Create(ctx context.Context, msg *schema.Message) (*schema.Message, bool, error) {
	key := bson.M{"tenant_id": msg.TenantID, "channel_id": msg.ChannelID, "sender_id": msg.SenderID, "client_message_id": msg.ClientMessageID}
	existing := func() (*schema.Message, error) {
		var m schema.Message
		err := r.messages.FindOne(ctx, key).Decode(&m)
		if err == nil && m.RequestHash != msg.RequestHash {
			return nil, response.ErrConflict
		}
		return &m, err
	}
	if m, err := existing(); err == nil {
		return m, false, nil
	} else if err != mongo.ErrNoDocuments {
		return nil, false, err
	}
	session, err := r.db.Client().StartSession()
	if err != nil {
		return nil, false, err
	}
	defer session.EndSession(ctx)
	msg.ID = primitive.NewObjectID()
	msg.CreatedAt = time.Now().UTC()
	msg.UpdatedAt = msg.CreatedAt
	msg.Revision = 1
	_, err = session.WithTransaction(ctx, func(sc mongo.SessionContext) (any, error) {
		var ch schema.Channel
		e := r.channels.FindOneAndUpdate(sc, bson.M{"_id": msg.ChannelID, "tenant_id": msg.TenantID, "active": true, "participant_ids": msg.SenderID}, bson.M{"$inc": bson.M{"last_message_seq": 1}, "$set": bson.M{"last_message_id": msg.ID, "last_message_at": msg.CreatedAt, "updated_at": msg.CreatedAt}}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&ch)
		if e == mongo.ErrNoDocuments {
			return nil, response.ErrNotFound
		}
		if e != nil {
			return nil, e
		}
		msg.Sequence = ch.LastMessageSeq
		_, e = r.messages.InsertOne(sc, msg)
		return nil, e
	}, options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()))
	if mongo.IsDuplicateKeyError(err) {
		m, e := existing()
		return m, false, e
	}
	if err != nil {
		return nil, false, err
	}
	return msg, true, nil
}
func (r *Repository) Get(ctx context.Context, tenant string, id primitive.ObjectID) (*schema.Message, error) {
	var msg schema.Message
	err := r.messages.FindOne(ctx, bson.M{"tenant_id": tenant, "_id": id}).Decode(&msg)
	if err == mongo.ErrNoDocuments {
		return nil, response.ErrNotFound
	}
	return &msg, err
}
func (r *Repository) List(ctx context.Context, tenant string, ch primitive.ObjectID, limit, before int64) ([]schema.Message, error) {
	filter := bson.M{"tenant_id": tenant, "channel_id": ch}
	if before > 0 {
		filter["seq"] = bson.M{"$lt": before}
	}
	cursor, err := r.messages.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "seq", Value: -1}}).SetLimit(limit))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	result := make([]schema.Message, 0)
	err = cursor.All(ctx, &result)
	return result, err
}
func (r *Repository) mutate(ctx context.Context, msg *schema.Message, fields bson.M) (*schema.Message, error) {
	fields["updated_at"] = time.Now().UTC()
	var updated schema.Message
	err := r.messages.FindOneAndUpdate(ctx, bson.M{"_id": msg.ID, "tenant_id": msg.TenantID, "sender_id": msg.SenderID, "revision": msg.Revision, "recalled_at": nil}, bson.M{"$set": fields, "$inc": bson.M{"revision": 1}}, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&updated)
	if err == mongo.ErrNoDocuments {
		return nil, response.ErrConflict
	}
	return &updated, err
}
func (r *Repository) Edit(ctx context.Context, msg *schema.Message, content string) (*schema.Message, error) {
	return r.mutate(ctx, msg, bson.M{"content": content})
}
func (r *Repository) Recall(ctx context.Context, msg *schema.Message) (*schema.Message, error) {
	return r.mutate(ctx, msg, bson.M{"content": "", "recalled_at": time.Now().UTC()})
}
func (r *Repository) Read(ctx context.Context, tenant string, ch primitive.ObjectID, user string, seq int64) (int64, error) {
	key := bson.M{"tenant_id": tenant, "channel_id": ch, "user_id": user}
	update := bson.M{"$max": bson.M{"last_read_seq": seq}, "$set": bson.M{"updated_at": time.Now().UTC()}, "$setOnInsert": key}
	var state schema.ChannelState
	err := r.states.FindOneAndUpdate(ctx, key, update, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&state)
	if mongo.IsDuplicateKeyError(err) {
		err = r.states.FindOneAndUpdate(ctx, key, update, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&state)
	}
	return state.LastReadSeq, err
}
