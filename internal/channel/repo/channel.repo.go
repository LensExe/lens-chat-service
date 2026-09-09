package repo

import (
	"context"
	"go-app/internal/schema"
	"go-app/pkg/response"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type Repository struct{ collection *mongo.Collection }

func New(db *mongo.Database) *Repository {
	return &Repository{db.Collection(schema.CollectionChannels)}
}
func (r *Repository) EnsureIndexes(ctx context.Context) error {
	_, err := r.collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "direct_key", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "tenant_id", Value: 1}, {Key: "participant_ids", Value: 1}, {Key: "updated_at", Value: -1}}},
	})
	return err
}
func (r *Repository) GetOrCreate(ctx context.Context, tenant string, pair [2]string, key string) (*schema.Channel, error) {
	filter := bson.M{"tenant_id": tenant, "direct_key": key}
	now := time.Now().UTC()
	value := schema.Channel{ID: primitive.NewObjectID(), TenantID: tenant, DirectKey: key, ParticipantIDs: pair, Active: true, CreatedAt: now, UpdatedAt: now}
	var ch schema.Channel
	err := r.collection.FindOneAndUpdate(ctx, filter, bson.M{"$setOnInsert": value}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&ch)
	if mongo.IsDuplicateKeyError(err) {
		err = r.collection.FindOne(ctx, filter).Decode(&ch)
	}
	return &ch, err
}
func (r *Repository) Get(ctx context.Context, tenant string, id primitive.ObjectID) (*schema.Channel, error) {
	var ch schema.Channel
	err := r.collection.FindOne(ctx, bson.M{"_id": id, "tenant_id": tenant}).Decode(&ch)
	if err == mongo.ErrNoDocuments {
		return nil, response.ErrNotFound
	}
	return &ch, err
}
func (r *Repository) List(ctx context.Context, tenant, user string, limit int64) ([]schema.Channel, error) {
	cursor, err := r.collection.Find(ctx, bson.M{"tenant_id": tenant, "participant_ids": user, "active": true}, options.Find().SetSort(bson.D{{Key: "updated_at", Value: -1}, {Key: "_id", Value: -1}}).SetLimit(limit))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	result := make([]schema.Channel, 0)
	err = cursor.All(ctx, &result)
	return result, err
}
