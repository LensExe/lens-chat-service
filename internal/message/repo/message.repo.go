package repo

import (
	"context"
	"go-app/internal/schema"
	"go-app/pkg/response"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Repository struct {
	messages, channels, states, outbox *mongo.Collection
}

func New(db *mongo.Database) *Repository {
	return &Repository{
		messages: db.Collection(schema.CollectionMessages),
		channels: db.Collection(schema.CollectionChannels),
		states:   db.Collection(schema.CollectionChannelStates),
		outbox:   db.Collection(schema.CollectionOutboxEvents),
	}
}

func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.messages.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "channel_id", Value: 1}, {Key: "seq", Value: -1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "channel_id", Value: 1}, {Key: "sender_id", Value: 1}, {Key: "client_message_id", Value: 1}}, Options: options.Index().SetUnique(true)},
	})
	if err != nil {
		return err
	}
	_, err = r.states.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "channel_id", Value: 1}, {Key: "user_id", Value: 1}}, Options: options.Index().SetUnique(true)})
	return err
}

func (r *Repository) Create(ctx context.Context, msg *schema.Message) (*schema.Message, bool, error) {
	key := bson.M{"channel_id": msg.ChannelID, "sender_id": msg.SenderID, "client_message_id": msg.ClientMessageID}
	existing := func() (*schema.Message, error) {
		var m schema.Message
		err := r.messages.FindOne(ctx, key).Decode(&m)
		if err == nil && m.RequestHash != msg.RequestHash {
			return nil, response.ErrConflict
		}
		if err != nil {
			return nil, err
		}
		return &m, nil
	}
	if m, err := existing(); err == nil {
		return m, false, nil
	} else if err != mongo.ErrNoDocuments {
		return nil, false, err
	}
	msg.ID = primitive.NewObjectID()
	msg.CreatedAt = time.Now().UTC()
	msg.UpdatedAt = msg.CreatedAt
	msg.Revision = 1
	eventID := uuid.NewString()
	outboxID := primitive.NewObjectID()
	var ch schema.Channel
	for attempt := 0; attempt < 64; attempt++ {
		if err := r.channels.FindOne(ctx, bson.M{
			"_id": msg.ChannelID, "active": true, "participant_ids": msg.SenderID,
		}).Decode(&ch); err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, false, response.ErrNotFound
			}
			return nil, false, err
		}

		sequence := ch.LastMessageSeq
		var latest schema.Message
		err := r.messages.FindOne(
			ctx,
			bson.M{"channel_id": msg.ChannelID},
			options.FindOne().SetSort(bson.D{{Key: "seq", Value: -1}}),
		).Decode(&latest)
		if err != nil && err != mongo.ErrNoDocuments {
			return nil, false, err
		}
		if err == nil && latest.Sequence > sequence {
			sequence = latest.Sequence
		}

		candidate := *msg
		candidate.Sequence = sequence + 1
		if _, err = r.messages.InsertOne(ctx, &candidate); err != nil {
			if !mongo.IsDuplicateKeyError(err) {
				return nil, false, err
			}
			if m, lookupErr := existing(); lookupErr == nil {
				return m, false, nil
			} else if lookupErr != mongo.ErrNoDocuments {
				return nil, false, lookupErr
			}
			// Another message took this sequence between the read and insert.
			// Retry from the newest persisted message; the unique sequence index
			// arbitrates concurrent writers on standalone MongoDB.
			continue
		}
		*msg = candidate
		break
	}
	if msg.Sequence == 0 {
		return nil, false, response.ErrConflict
	}

	// Mongo standalone can atomically update one channel document, but cannot
	// commit this update, the message insert, and the outbox insert together.
	// The summary uses max(sequence) so delayed concurrent writers cannot make
	// the channel point back to an older message.
	currentSequence := bson.M{"$ifNull": bson.A{"$last_message_seq", int64(0)}}
	setFields := bson.M{
		"last_message_seq": bson.M{"$max": bson.A{currentSequence, msg.Sequence}},
		"last_message_id": bson.M{"$cond": bson.A{
			bson.M{"$lt": bson.A{currentSequence, msg.Sequence}}, msg.ID, "$last_message_id",
		}},
		"last_message_at": bson.M{"$cond": bson.A{
			bson.M{"$lt": bson.A{currentSequence, msg.Sequence}}, msg.CreatedAt, "$last_message_at",
		}},
		"last_event_version": bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$last_event_version", int64(0)}}, int64(1)}},
		"updated_at":         msg.UpdatedAt,
	}
	if err := r.channels.FindOneAndUpdate(
		ctx,
		bson.M{"_id": msg.ChannelID, "active": true, "participant_ids": msg.SenderID},
		mongo.Pipeline{bson.D{{Key: "$set", Value: setFields}}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&ch); err != nil {
		if err == mongo.ErrNoDocuments {
			_, _ = r.messages.DeleteOne(ctx, bson.M{"_id": msg.ID})
			return nil, false, response.ErrNotFound
		}
		return nil, false, err
	}

	occurredAt := time.Now().UTC()
	channelID := ch.ID.Hex()
	event := schema.ChatEventEnvelope{
		EventID:          eventID,
		EventType:        schema.EventChatMessageCreated,
		SchemaVersion:    1,
		OccurredAt:       occurredAt,
		Producer:         "chat-service",
		AggregateType:    "channel",
		AggregateID:      channelID,
		AggregateVersion: ch.LastEventVersion,
		Data: schema.ChatEventData{
			ChannelID:      channelID,
			ParticipantIDs: ch.ParticipantIDs,
			Message:        messageEventPayload(msg),
		},
	}
	_, err := r.outbox.InsertOne(ctx, schema.OutboxEvent{
		ID:               outboxID,
		EventID:          eventID,
		Event:            event,
		PartitionKey:     channelID,
		AggregateVersion: ch.LastEventVersion,
		Status:           schema.OutboxStatusPending,
		AttemptCount:     0,
		NextAttemptAt:    occurredAt,
		CreatedAt:        occurredAt,
	})
	if err != nil {
		return nil, false, err
	}
	return msg, true, nil
}

func hexIfSet(id primitive.ObjectID) string {
	if id.IsZero() {
		return ""
	}
	return id.Hex()
}

func messageEventPayload(msg *schema.Message) *schema.ChatMessageEventPayload {
	return &schema.ChatMessageEventPayload{
		ID:               msg.ID.Hex(),
		ChannelID:        msg.ChannelID.Hex(),
		SenderID:         msg.SenderID,
		ClientMessageID:  msg.ClientMessageID,
		Type:             msg.Type,
		Content:          msg.Content,
		Sequence:         msg.Sequence,
		ReplyToMessageID: hexIfSet(msg.ReplyToMessageID),
		Revision:         msg.Revision,
		RecalledAt:       msg.RecalledAt,
		CreatedAt:        msg.CreatedAt,
		UpdatedAt:        msg.UpdatedAt,
	}
}

func (r *Repository) Get(ctx context.Context, id primitive.ObjectID) (*schema.Message, error) {
	var msg schema.Message
	err := r.messages.FindOne(ctx, bson.M{"_id": id}).Decode(&msg)
	if err == mongo.ErrNoDocuments {
		return nil, response.ErrNotFound
	}
	return &msg, err
}

func (r *Repository) List(ctx context.Context, ch primitive.ObjectID, limit, before int64) ([]schema.Message, error) {
	filter := bson.M{"channel_id": ch}
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

func (r *Repository) mutate(
	ctx context.Context,
	msg *schema.Message,
	fields bson.M,
	eventType string,
) (*schema.Message, error) {
	now := time.Now().UTC()
	eventID := uuid.NewString()
	outboxID := primitive.NewObjectID()
	var updated schema.Message
	updatedFields := bson.M{}
	for key, value := range fields {
		updatedFields[key] = value
	}
	updatedFields["updated_at"] = now
	if eventType == schema.EventChatMessageRecalled {
		updatedFields["recalled_at"] = now
	}
	if err := r.messages.FindOneAndUpdate(
		ctx,
		bson.M{"_id": msg.ID, "sender_id": msg.SenderID, "revision": msg.Revision, "recalled_at": nil},
		bson.M{"$set": updatedFields, "$inc": bson.M{"revision": 1}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&updated); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, response.ErrConflict
		}
		return nil, err
	}

	var ch schema.Channel
	if err := r.channels.FindOneAndUpdate(
		ctx,
		bson.M{"_id": updated.ChannelID, "active": true},
		bson.M{"$inc": bson.M{"last_event_version": 1}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&ch); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, response.ErrNotFound
		}
		return nil, err
	}

	channelID := ch.ID.Hex()
	event := schema.ChatEventEnvelope{
		EventID:          eventID,
		EventType:        eventType,
		SchemaVersion:    1,
		OccurredAt:       now,
		Producer:         "chat-service",
		AggregateType:    "channel",
		AggregateID:      channelID,
		AggregateVersion: ch.LastEventVersion,
		Data: schema.ChatEventData{
			ChannelID:      channelID,
			ParticipantIDs: ch.ParticipantIDs,
			Message:        messageEventPayload(&updated),
		},
	}
	_, err := r.outbox.InsertOne(ctx, schema.OutboxEvent{
		ID:               outboxID,
		EventID:          eventID,
		Event:            event,
		PartitionKey:     channelID,
		AggregateVersion: ch.LastEventVersion,
		Status:           schema.OutboxStatusPending,
		NextAttemptAt:    now,
		CreatedAt:        now,
	})
	if err != nil {
		return nil, err
	}
	return &updated, nil
}

func (r *Repository) Edit(ctx context.Context, msg *schema.Message, content string) (*schema.Message, error) {
	return r.mutate(ctx, msg, bson.M{"content": content}, schema.EventChatMessageUpdated)
}

func (r *Repository) Recall(ctx context.Context, msg *schema.Message) (*schema.Message, error) {
	return r.mutate(ctx, msg, bson.M{"content": ""}, schema.EventChatMessageRecalled)
}

func (r *Repository) Read(ctx context.Context, ch primitive.ObjectID, user string, seq int64) (int64, error) {
	key := bson.M{"channel_id": ch, "user_id": user}
	now := time.Now().UTC()
	eventID := uuid.NewString()
	outboxID := primitive.NewObjectID()
	filter := bson.M{
		"channel_id": ch,
		"user_id":    user,
		"$or": bson.A{
			bson.M{"last_read_seq": bson.M{"$lt": seq}},
			bson.M{"last_read_seq": bson.M{"$exists": false}},
		},
	}
	result, err := r.states.UpdateOne(ctx, filter, bson.M{
		"$set":         bson.M{"last_read_seq": seq, "updated_at": now},
		"$setOnInsert": key,
	}, options.Update().SetUpsert(true))
	advanced := err == nil && (result.MatchedCount > 0 || result.UpsertedCount > 0)
	if mongo.IsDuplicateKeyError(err) {
		// A concurrent upsert advanced the same cursor first. Read its final
		// value below; this request must not emit another event.
		err = nil
	}
	if err != nil {
		return 0, err
	}
	var state schema.ChannelState
	if err := r.states.FindOne(ctx, key).Decode(&state); err != nil {
		return 0, err
	}
	if !advanced {
		return state.LastReadSeq, nil
	}
	lastReadSeq := state.LastReadSeq

	var channel schema.Channel
	if err := r.channels.FindOneAndUpdate(
		ctx,
		bson.M{"_id": ch, "active": true, "participant_ids": user},
		bson.M{"$inc": bson.M{"last_event_version": 1}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&channel); err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, response.ErrNotFound
		}
		return 0, err
	}
	channelID := channel.ID.Hex()
	version := channel.LastEventVersion
	event := schema.ChatEventEnvelope{
		EventID:          eventID,
		EventType:        schema.EventChatReadCursorAdvanced,
		SchemaVersion:    1,
		OccurredAt:       now,
		Producer:         "chat-service",
		AggregateType:    "channel",
		AggregateID:      channelID,
		AggregateVersion: version,
		Data: schema.ChatEventData{
			ChannelID:      channelID,
			ParticipantIDs: channel.ParticipantIDs,
			UserID:         user,
			LastReadSeq:    &lastReadSeq,
		},
	}
	_, err = r.outbox.InsertOne(ctx, schema.OutboxEvent{
		ID:               outboxID,
		EventID:          eventID,
		Event:            event,
		PartitionKey:     channelID,
		AggregateVersion: version,
		Status:           schema.OutboxStatusPending,
		NextAttemptAt:    now,
		CreatedAt:        now,
	})
	return lastReadSeq, err
}
