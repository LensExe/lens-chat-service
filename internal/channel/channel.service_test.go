package channel

import (
	"context"
	"errors"
	"go-app/internal/schema"
	"go-app/pkg/response"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
)

type fakeRepo struct {
	ch  *schema.Channel
	key string
}

func (f *fakeRepo) GetOrCreate(_ context.Context, pair [2]string, key string) (*schema.Channel, error) {
	f.key = key
	return &schema.Channel{ParticipantIDs: pair}, nil
}
func (f *fakeRepo) Get(_ context.Context, _ primitive.ObjectID) (*schema.Channel, error) {
	return f.ch, nil
}
func (f *fakeRepo) List(context.Context, string, int64) ([]schema.Channel, error) {
	return nil, nil
}
func TestDirectChannelBoundaries(t *testing.T) {
	ctx := context.Background()
	f := &fakeRepo{ch: &schema.Channel{ID: primitive.NewObjectID(), ParticipantIDs: [2]string{"a", "b"}, Active: true}}
	s := NewService(f)
	first, err := s.Create(ctx, "b", "a")
	if err != nil {
		t.Fatal(err)
	}
	key := f.key
	second, err := s.Create(ctx, "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	if first.ParticipantIDs != second.ParticipantIDs || key != f.key {
		t.Fatal("direct pair is not canonical")
	}
	for _, peer := range []string{"", "a", " b "} {
		if _, err = s.Create(ctx, "a", peer); !errors.Is(err, response.ErrInvalid) {
			t.Fatalf("accepted peer %q", peer)
		}
	}
	if _, err = s.Get(ctx, "outsider", f.ch.ID.Hex()); !errors.Is(err, response.ErrNotFound) {
		t.Fatal("cross-user access allowed")
	}
	if _, err = s.Get(ctx, "a", f.ch.ID.Hex()); err != nil {
		t.Fatal(err)
	}
}
