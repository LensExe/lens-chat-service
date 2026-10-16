package message

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go-app/internal/channel"
	"go-app/internal/dto"
	"go-app/internal/schema"
	"go-app/pkg/response"
	"strings"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Repository interface {
	Create(context.Context, *schema.Message) (*schema.Message, bool, error)
	Get(context.Context, primitive.ObjectID) (*schema.Message, error)
	List(context.Context, primitive.ObjectID, int64, int64) ([]schema.Message, error)
	Edit(context.Context, *schema.Message, string) (*schema.Message, error)
	Recall(context.Context, *schema.Message) (*schema.Message, error)
	Read(context.Context, primitive.ObjectID, string, int64) (int64, error)
}
type Service struct {
	repo     Repository
	channels *channel.Service
}

func NewService(repo Repository, channels *channel.Service) *Service {
	return &Service{repo: repo, channels: channels}
}

func (s *Service) Create(
	ctx context.Context,
	actor,
	id string,
	req dto.CreateMessageRequest,
) (*schema.Message, bool, error) {
	ch, err := s.channels.Get(ctx, actor, id)
	if err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(req.ClientMessageID) == "" || len(req.ClientMessageID) > 100 || !utf8.ValidString(req.Content) || strings.TrimSpace(req.Content) == "" || len(req.Content) > 10000 || (req.Type != "text" && req.Type != "image" && req.Type != "file") {
		return nil, false, response.ErrInvalid
	}
	msg := &schema.Message{ChannelID: ch.ID, SenderID: actor, SenderKind: "user", ClientMessageID: req.ClientMessageID, Type: req.Type, Content: req.Content}
	if req.ReplyToMessageID != "" {
		replyID, e := primitive.ObjectIDFromHex(req.ReplyToMessageID)
		if e != nil {
			return nil, false, response.ErrInvalid
		}
		reply, e := s.repo.Get(ctx, replyID)
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
	return saved, created, nil
}

func (s *Service) CreateAIReply(
	ctx context.Context,
	channelID string,
	photographerID string,
	req dto.CreateAIReplyRequest,
) (*schema.Message, bool, error) {
	if strings.TrimSpace(photographerID) == "" ||
		strings.TrimSpace(req.ClientMessageID) == "" ||
		len(req.ClientMessageID) > 100 ||
		strings.TrimSpace(req.Content) == "" ||
		len(req.Content) > 10000 ||
		!utf8.ValidString(req.Content) ||
		req.ExpectedLastSequence < 1 ||
		strings.TrimSpace(req.SourceMessageID) == "" {
		return nil, false, response.ErrInvalid
	}

	ch, err := s.channels.Get(ctx, photographerID, channelID)
	if err != nil {
		return nil, false, err
	}
	sourceID, err := primitive.ObjectIDFromHex(req.SourceMessageID)
	if err != nil {
		return nil, false, response.ErrInvalid
	}
	source, err := s.repo.Get(ctx, sourceID)
	if err != nil {
		return nil, false, err
	}
	if source.ChannelID != ch.ID ||
		source.SenderID == photographerID ||
		source.SenderKind == "ai" ||
		source.RecalledAt != nil {
		return nil, false, response.ErrConflict
	}

	msg := &schema.Message{
		ChannelID:        ch.ID,
		SenderID:         photographerID,
		SenderKind:       "ai",
		ClientMessageID:  req.ClientMessageID,
		Type:             "text",
		Content:          strings.TrimSpace(req.Content),
		ReplyToMessageID: source.ID,
	}
	encoded, _ := json.Marshal(req)
	msg.RequestHash = fmt.Sprintf("%x", sha256.Sum256(encoded))
	saved, created, err := s.repo.Create(ctx, msg)
	if err != nil {
		return nil, false, err
	}
	return saved, created, nil
}

func (s *Service) ListForAI(
	ctx context.Context,
	channelID string,
	photographerID string,
	limit int64,
	beforeSequence int64,
) ([]schema.Message, error) {
	if limit < 1 || limit > 30 || beforeSequence < 0 {
		return nil, response.ErrInvalid
	}
	ch, err := s.channels.Get(ctx, photographerID, channelID)
	if err != nil {
		return nil, err
	}
	messages, err := s.repo.List(ctx, ch.ID, limit, beforeSequence)
	if err != nil {
		return nil, err
	}
	for left, right := 0, len(messages)-1; left < right; left, right = left+1, right-1 {
		messages[left], messages[right] = messages[right], messages[left]
	}
	return messages, nil
}

func (s *Service) List(
	ctx context.Context,
	actor,
	id string,
	limit,
	before int64,
) ([]schema.Message, error) {
	if limit < 1 || limit > 100 || before < 0 {
		return nil, response.ErrInvalid
	}
	ch, err := s.channels.Get(ctx, actor, id)
	if err != nil {
		return nil, err
	}
	return s.repo.List(ctx, ch.ID, limit, before)
}

func (s *Service) authorized(
	ctx context.Context,
	actor,
	id string,
) (*schema.Message, *schema.Channel, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, nil, response.ErrInvalid
	}
	msg, err := s.repo.Get(ctx, oid)
	if err != nil {
		return nil, nil, err
	}
	ch, err := s.channels.Get(ctx, actor, msg.ChannelID.Hex())
	if err != nil {
		return nil, nil, err
	}
	if msg.SenderID != actor {
		return nil, nil, response.ErrForbidden
	}
	return msg, ch, nil
}

func (s *Service) Edit(
	ctx context.Context,
	actor,
	id,
	content string,
) (*schema.Message, error) {
	if strings.TrimSpace(content) == "" || len(content) > 10000 || !utf8.ValidString(content) {
		return nil, response.ErrInvalid
	}
	msg, _, err := s.authorized(ctx, actor, id)
	if err != nil {
		return nil, err
	}
	if msg.RecalledAt != nil {
		return nil, response.ErrConflict
	}
	updated, err := s.repo.Edit(ctx, msg, content)
	return updated, err
}

func (s *Service) Recall(ctx context.Context, actor, id string) (*schema.Message, error) {
	msg, _, err := s.authorized(ctx, actor, id)
	if err != nil {
		return nil, err
	}
	if msg.RecalledAt != nil {
		return msg, nil
	}
	updated, err := s.repo.Recall(ctx, msg)
	return updated, err
}

func (s *Service) Read(
	ctx context.Context,
	actor,
	id string,
	seq int64,
) (int64, error) {
	ch, err := s.channels.Get(ctx, actor, id)
	if err != nil {
		return 0, err
	}
	if seq < 0 || seq > ch.LastMessageSeq {
		return 0, response.ErrInvalid
	}
	current, err := s.repo.Read(ctx, ch.ID, actor, seq)
	return current, err
}
