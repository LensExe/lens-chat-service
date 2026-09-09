package message

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go-app/internal/channel"
	"go-app/internal/dto"
	"go-app/internal/schema"
	"go-app/pkg/mapper"
	"go-app/pkg/response"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strings"
	"unicode/utf8"
)

type Repository interface {
	Create(context.Context, *schema.Message) (*schema.Message, bool, error)
	Get(context.Context, string, primitive.ObjectID) (*schema.Message, error)
	List(context.Context, string, primitive.ObjectID, int64, int64) ([]schema.Message, error)
	Edit(context.Context, *schema.Message, string) (*schema.Message, error)
	Recall(context.Context, *schema.Message) (*schema.Message, error)
	Read(context.Context, string, primitive.ObjectID, string, int64) (int64, error)
}
type Notifier interface {
	Notify(string, [2]string, string, any)
}
type Service struct {
	repo     Repository
	channels *channel.Service
	notifier Notifier
}

func NewService(repo Repository, channels *channel.Service, notifier Notifier) *Service {
	return &Service{repo, channels, notifier}
}
func (s *Service) Create(ctx context.Context, tenant, actor, id string, req dto.CreateMessageRequest) (*schema.Message, bool, error) {
	ch, err := s.channels.Get(ctx, tenant, actor, id)
	if err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(req.ClientMessageID) == "" || len(req.ClientMessageID) > 100 || !utf8.ValidString(req.Content) || strings.TrimSpace(req.Content) == "" || len(req.Content) > 10000 || (req.Type != "text" && req.Type != "image" && req.Type != "file") {
		return nil, false, response.ErrInvalid
	}
	msg := &schema.Message{TenantID: tenant, ChannelID: ch.ID, SenderID: actor, ClientMessageID: req.ClientMessageID, Type: req.Type, Content: req.Content}
	if req.ReplyToMessageID != "" {
		replyID, e := primitive.ObjectIDFromHex(req.ReplyToMessageID)
		if e != nil {
			return nil, false, response.ErrInvalid
		}
		reply, e := s.repo.Get(ctx, tenant, replyID)
		if e != nil {
			return nil, false, e
		}
		if reply.ChannelID != ch.ID {
			return nil, false, response.ErrInvalid
		}
		msg.ReplyToMessageID = replyID
	}
	encoded, _ := json.Marshal(req)
	msg.RequestHash = fmt.Sprintf("%x", sha256.Sum256(encoded))
	saved, created, err := s.repo.Create(ctx, msg)
	if err != nil {
		return nil, false, err
	}
	if created && s.notifier != nil {
		s.notifier.Notify(tenant, ch.ParticipantIDs, "NEW_MESSAGE", mapper.Message(saved))
	}
	return saved, created, nil
}
func (s *Service) List(ctx context.Context, tenant, actor, id string, limit, before int64) ([]schema.Message, error) {
	if limit < 1 || limit > 100 || before < 0 {
		return nil, response.ErrInvalid
	}
	ch, err := s.channels.Get(ctx, tenant, actor, id)
	if err != nil {
		return nil, err
	}
	return s.repo.List(ctx, tenant, ch.ID, limit, before)
}
func (s *Service) authorized(ctx context.Context, tenant, actor, id string) (*schema.Message, *schema.Channel, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, nil, response.ErrInvalid
	}
	msg, err := s.repo.Get(ctx, tenant, oid)
	if err != nil {
		return nil, nil, err
	}
	ch, err := s.channels.Get(ctx, tenant, actor, msg.ChannelID.Hex())
	if err != nil {
		return nil, nil, err
	}
	if msg.SenderID != actor {
		return nil, nil, response.ErrForbidden
	}
	return msg, ch, nil
}
func (s *Service) Edit(ctx context.Context, tenant, actor, id, content string) (*schema.Message, error) {
	if strings.TrimSpace(content) == "" || len(content) > 10000 || !utf8.ValidString(content) {
		return nil, response.ErrInvalid
	}
	msg, ch, err := s.authorized(ctx, tenant, actor, id)
	if err != nil {
		return nil, err
	}
	if msg.RecalledAt != nil {
		return nil, response.ErrConflict
	}
	updated, err := s.repo.Edit(ctx, msg, content)
	if err == nil && s.notifier != nil {
		s.notifier.Notify(tenant, ch.ParticipantIDs, "UPDATED_MESSAGE", mapper.Message(updated))
	}
	return updated, err
}
func (s *Service) Recall(ctx context.Context, tenant, actor, id string) (*schema.Message, error) {
	msg, ch, err := s.authorized(ctx, tenant, actor, id)
	if err != nil {
		return nil, err
	}
	if msg.RecalledAt != nil {
		return msg, nil
	}
	updated, err := s.repo.Recall(ctx, msg)
	if err == nil && s.notifier != nil {
		s.notifier.Notify(tenant, ch.ParticipantIDs, "RECALLED_MESSAGE", mapper.Message(updated))
	}
	return updated, err
}
func (s *Service) Read(ctx context.Context, tenant, actor, id string, seq int64) (int64, error) {
	ch, err := s.channels.Get(ctx, tenant, actor, id)
	if err != nil {
		return 0, err
	}
	if seq < 0 || seq > ch.LastMessageSeq {
		return 0, response.ErrInvalid
	}
	current, err := s.repo.Read(ctx, tenant, ch.ID, actor, seq)
	if err == nil && s.notifier != nil {
		s.notifier.Notify(tenant, ch.ParticipantIDs, "READ_CURSOR", map[string]any{"channel_id": id, "user_id": actor, "last_read_seq": current})
	}
	return current, err
}
