package channel

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go-app/internal/schema"
	"go-app/pkg/response"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"sort"
	"strings"
)

type Repository interface {
	GetOrCreate(context.Context, string, [2]string, string) (*schema.Channel, error)
	Get(context.Context, string, primitive.ObjectID) (*schema.Channel, error)
	List(context.Context, string, string, int64) ([]schema.Channel, error)
}
type Service struct{ repo Repository }

func NewService(repo Repository) *Service { return &Service{repo: repo} }
func (s *Service) Create(ctx context.Context, tenant, actor, peer string) (*schema.Channel, error) {
	if actor == "" || strings.TrimSpace(peer) == "" || peer != strings.TrimSpace(peer) || len(peer) > 255 || actor == peer {
		return nil, response.ErrInvalid
	}
	pair := []string{actor, peer}
	sort.Strings(pair)
	encoded, _ := json.Marshal(pair)
	return s.repo.GetOrCreate(ctx, tenant, [2]string{pair[0], pair[1]}, fmt.Sprintf("%x", sha256.Sum256(encoded)))
}
func (s *Service) Get(ctx context.Context, tenant, actor, id string) (*schema.Channel, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, response.ErrInvalid
	}
	ch, err := s.repo.Get(ctx, tenant, oid)
	if err != nil {
		return nil, err
	}
	if !ch.Active || (ch.ParticipantIDs[0] != actor && ch.ParticipantIDs[1] != actor) {
		return nil, response.ErrNotFound
	}
	return ch, nil
}
func (s *Service) List(ctx context.Context, tenant, actor string) ([]schema.Channel, error) {
	return s.repo.List(ctx, tenant, actor, 100)
}
