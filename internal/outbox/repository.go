package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go-app/internal/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const relayLeaseID = "chat-outbox-relay"

type Repository struct {
	events *mongo.Collection
	leases *mongo.Collection
}

func NewRepository(db *mongo.Database) *Repository {
	return &Repository{
		events: db.Collection(schema.CollectionOutboxEvents),
		leases: db.Collection(schema.CollectionOutboxRelayLeases),
	}
}

func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.events.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "event_id", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "status", Value: 1}, {Key: "partition_key", Value: 1}, {Key: "event.aggregate_version", Value: 1}},
		},
	})
	return err
}

// AcquireRelayLease elects one active relay per Chat database. The active relay
// may publish different channel heads concurrently while preserving each
// channel's aggregate_version order.
func (r *Repository) AcquireRelayLease(ctx context.Context, owner string, now time.Time, duration time.Duration) (bool, error) {
	filter := bson.M{
		"_id": relayLeaseID,
		"$or": bson.A{
			bson.M{"lease_until": bson.M{"$lte": now}},
			bson.M{"owner": owner},
			bson.M{"lease_until": bson.M{"$exists": false}},
		},
	}
	update := bson.M{
		"$set": bson.M{
			"owner":       owner,
			"lease_until": now.Add(duration),
			"updated_at":  now,
		},
	}
	var lease schema.OutboxRelayLease
	err := r.leases.FindOneAndUpdate(
		ctx,
		filter,
		update,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	).Decode(&lease)
	if mongo.IsDuplicateKeyError(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return lease.Owner == owner, nil
}

func (r *Repository) ReleaseRelayLease(ctx context.Context, owner string) error {
	_, err := r.leases.DeleteOne(ctx, bson.M{"_id": relayLeaseID, "owner": owner})
	return err
}

// PendingChannelHeads returns the oldest pending event for each channel,
// ordered by event creation time. A delayed event blocks only its own channel.
func (r *Repository) PendingChannelHeads(ctx context.Context, limit int64) ([]schema.OutboxEvent, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"status": schema.OutboxStatusPending}}},
		{{Key: "$sort", Value: bson.D{
			{Key: "partition_key", Value: 1},
			{Key: "event.aggregate_version", Value: 1},
			{Key: "created_at", Value: 1},
			{Key: "_id", Value: 1},
		}}},
		{{Key: "$group", Value: bson.M{
			"_id":   "$partition_key",
			"event": bson.M{"$first": "$$ROOT"},
		}}},
		{{Key: "$replaceRoot", Value: bson.M{"newRoot": "$event"}}},
		{{Key: "$sort", Value: bson.D{{Key: "created_at", Value: 1}, {Key: "_id", Value: 1}}}},
		{{Key: "$limit", Value: limit}},
	}
	cursor, err := r.events.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var events []schema.OutboxEvent
	if err = cursor.All(ctx, &events); err != nil {
		return nil, err
	}
	return events, nil
}

func (r *Repository) MarkPublished(ctx context.Context, id primitive.ObjectID, now time.Time) error {
	result, err := r.events.UpdateOne(
		ctx,
		bson.M{"_id": id, "status": schema.OutboxStatusPending},
		bson.M{
			"$set":   bson.M{"status": schema.OutboxStatusPublished, "published_at": now},
			"$unset": bson.M{"last_error": ""},
		},
	)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return errors.New("outbox event was not pending when marking published")
	}
	return nil
}

func (r *Repository) ScheduleRetry(ctx context.Context, id primitive.ObjectID, retryAt time.Time, publishErr error) error {
	message := "unknown Kafka publish error"
	if publishErr != nil {
		message = strings.TrimSpace(publishErr.Error())
	}
	if len(message) > 512 {
		message = message[:512]
	}
	_, err := r.events.UpdateOne(
		ctx,
		bson.M{"_id": id, "status": schema.OutboxStatusPending},
		bson.M{
			"$set": bson.M{"next_attempt_at": retryAt, "last_error": message},
			"$inc": bson.M{"attempt_count": 1},
		},
	)
	return err
}

func (r *Repository) MarkFailed(ctx context.Context, id primitive.ObjectID, now time.Time, attempts int64, publishErr error) error {
	message := "unknown Kafka publish error"
	if publishErr != nil {
		message = strings.TrimSpace(publishErr.Error())
	}
	if len(message) > 512 {
		message = message[:512]
	}
	result, err := r.events.UpdateOne(
		ctx,
		bson.M{"_id": id, "status": schema.OutboxStatusPending},
		bson.M{"$set": bson.M{
			"status":        schema.OutboxStatusFailed,
			"failed_at":     now,
			"attempt_count": attempts,
			"last_error":    message,
		}},
	)
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return errors.New("outbox event was not pending when marking failed")
	}
	return nil
}

func (r *Repository) RenderMetrics(ctx context.Context, telemetry *Telemetry) (string, error) {
	pending, err := r.events.CountDocuments(ctx, bson.M{"status": schema.OutboxStatusPending})
	if err != nil {
		return "", err
	}
	failed, err := r.events.CountDocuments(ctx, bson.M{"status": schema.OutboxStatusFailed})
	if err != nil {
		return "", err
	}
	oldestAge := 0.0
	var oldest schema.OutboxEvent
	if err = r.events.FindOne(
		ctx,
		bson.M{"status": schema.OutboxStatusPending},
		options.FindOne().SetSort(bson.D{{Key: "created_at", Value: 1}, {Key: "_id", Value: 1}}),
	).Decode(&oldest); err == nil {
		oldestAge = time.Since(oldest.CreatedAt).Seconds()
	} else if err != mongo.ErrNoDocuments {
		return "", err
	}

	var snapshot telemetrySnapshot
	if telemetry != nil {
		snapshot = telemetry.snapshot()
	}
	var output strings.Builder
	fmt.Fprintf(&output, "# TYPE chat_outbox_pending gauge\nchat_outbox_pending %d\n", pending)
	fmt.Fprintf(&output, "# TYPE chat_outbox_failed gauge\nchat_outbox_failed %d\n", failed)
	fmt.Fprintf(&output, "# TYPE chat_outbox_oldest_pending_age_seconds gauge\nchat_outbox_oldest_pending_age_seconds %.6f\n", oldestAge)
	fmt.Fprintf(&output, "# TYPE chat_outbox_publish_attempts_total counter\nchat_outbox_publish_attempts_total %d\n", snapshot.attempts)
	fmt.Fprintf(&output, "# TYPE chat_outbox_published_total counter\nchat_outbox_published_total %d\n", snapshot.published)
	fmt.Fprintf(&output, "# TYPE chat_outbox_publish_failures_total counter\nchat_outbox_publish_failures_total %d\n", snapshot.publishFailures)
	fmt.Fprintf(&output, "# TYPE chat_outbox_retries_total counter\nchat_outbox_retries_total %d\n", snapshot.retries)
	fmt.Fprintf(&output, "# TYPE chat_outbox_parked_total counter\nchat_outbox_parked_total %d\n", snapshot.parked)
	fmt.Fprintf(&output, "# TYPE chat_outbox_publish_duration_seconds summary\nchat_outbox_publish_duration_seconds_count %d\nchat_outbox_publish_duration_seconds_sum %.6f\n", snapshot.processingCount, float64(snapshot.processingNanos)/float64(time.Second))
	return output.String(), nil
}
