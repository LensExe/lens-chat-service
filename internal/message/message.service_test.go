package message

import (
	"context"
	"errors"
	"go-app/internal/channel"
	"go-app/internal/dto"
	"go-app/internal/schema"
	"go-app/pkg/response"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
)

type channelsFake struct {
	channel.Repository
	ch *schema.Channel
}

func (f channelsFake) Get(_ context.Context, tenant string, _ primitive.ObjectID) (*schema.Channel, error) {
	if tenant != f.ch.TenantID {
		return nil, response.ErrNotFound
	}
	return f.ch, nil
}

type messagesFake struct {
	Repository
	msg    *schema.Message
	writes int
}

func (f *messagesFake) Get(context.Context, string, primitive.ObjectID) (*schema.Message, error) {
	return f.msg, nil
}
func (f *messagesFake) Create(_ context.Context, m *schema.Message) (*schema.Message, bool, error) {
	f.writes++
	return m, true, nil
}
func TestMessageAuthorization(t *testing.T) {
	ctx := context.Background()
	id := primitive.NewObjectID()
	ch := &schema.Channel{ID: id, TenantID: "t", ParticipantIDs: [2]string{"a", "b"}, Active: true, LastMessageSeq: 10}
	repo := &messagesFake{msg: &schema.Message{ID: primitive.NewObjectID(), ChannelID: id, SenderID: "a"}}
	service := NewService(repo, channel.NewService(channelsFake{ch: ch}), nil)
	req := dto.CreateMessageRequest{ClientMessageID: "client-id", Type: "text", Content: "hello"}
	if _, _, err := service.Create(ctx, "t", "intruder", id.Hex(), req); !errors.Is(err, response.ErrNotFound) {
		t.Fatal("outsider sent message")
	}
	if _, err := service.Edit(ctx, "t", "b", repo.msg.ID.Hex(), "edited"); !errors.Is(err, response.ErrForbidden) {
		t.Fatal("recipient edited sender message")
	}
	if _, err := service.Recall(ctx, "t", "b", repo.msg.ID.Hex()); !errors.Is(err, response.ErrForbidden) {
		t.Fatal("recipient recalled sender message")
	}
	if _, err := service.Read(ctx, "t", "a", id.Hex(), 11); !errors.Is(err, response.ErrInvalid) {
		t.Fatal("accepted future read position")
	}
	if repo.writes != 0 {
		t.Fatal("unauthorized mutation reached repository")
	}
	if _, created, err := service.Create(ctx, "t", "a", id.Hex(), req); err != nil || !created {
		t.Fatalf("valid send: %v", err)
	}
	req.ReplyToMessageID = repo.msg.ID.Hex()
	repo.msg.ChannelID = primitive.NewObjectID()
	if _, _, err := service.Create(ctx, "t", "a", id.Hex(), req); !errors.Is(err, response.ErrInvalid) {
		t.Fatal("accepted cross-channel reply")
	}
}
