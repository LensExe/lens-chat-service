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

func (f *fakeRepo) GetOrCreate(_ context.Context, tenant string, pair [2]string, key string) (*schema.Channel, error) {
	f.key = key
	return &schema.Channel{ParticipantIDs: pair, TenantID: tenant}, nil
}
func (f *fakeRepo) Get(_ context.Context, tenant string, _ primitive.ObjectID) (*schema.Channel, error) {
	if tenant != f.ch.TenantID {
		return nil, response.ErrNotFound
	}
	return f.ch, nil
}
func (f *fakeRepo) List(context.Context, string, string, int64) ([]schema.Channel, error) {
	return nil, nil
}
func TestDirectChannelBoundaries(t *testing.T) {
	ctx := context.Background()
	f := &fakeRepo{ch: &schema.Channel{ID: primitive.NewObjectID(), TenantID: "tenant", ParticipantIDs: [2]string{"a", "b"}, Active: true}}
	s := NewService(f)
	first, err := s.Create(ctx, "tenant", "b", "a")
	if err != nil {
		t.Fatal(err)
	}
	key := f.key
	second, err := s.Create(ctx, "tenant", "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	if first.ParticipantIDs != second.ParticipantIDs || key != f.key {
		t.Fatal("direct pair is not canonical")
	}
	for _, peer := range []string{"", "a", " b "} {
		if _, err = s.Create(ctx, "tenant", "a", peer); !errors.Is(err, response.ErrInvalid) {
			t.Fatalf("accepted peer %q", peer)
		}
	}
	for _, tc := range []struct{ tenant, user string }{{"tenant", "outsider"}, {"other", "a"}} {
		if _, err = s.Get(ctx, tc.tenant, tc.user, f.ch.ID.Hex()); !errors.Is(err, response.ErrNotFound) {
			t.Fatal("cross-user/tenant access allowed")
		}
	}
	if _, err = s.Get(ctx, "tenant", "a", f.ch.ID.Hex()); err != nil {
		t.Fatal(err)
	}
}
