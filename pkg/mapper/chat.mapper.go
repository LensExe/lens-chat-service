package mapper

import (
	"go-app/internal/dto"
	"go-app/internal/schema"
)

func Channel(ch *schema.Channel, actor string) dto.ChannelResponse {
	peer := ch.ParticipantIDs[0]
	if peer == actor {
		peer = ch.ParticipantIDs[1]
	}
	result := dto.ChannelResponse{ID: ch.ID.Hex(), ParticipantIDs: ch.ParticipantIDs, PeerID: peer, LastMessageSeq: ch.LastMessageSeq, CreatedAt: ch.CreatedAt, UpdatedAt: ch.UpdatedAt}
	if !ch.LastMessageID.IsZero() {
		result.LastMessageID = ch.LastMessageID.Hex()
	}
	if ch.LastMessageAt != nil {
		result.LastMessageAt = *ch.LastMessageAt
	}
	return result
}
func Message(m *schema.Message) dto.MessageResponse {
	result := dto.MessageResponse{ID: m.ID.Hex(), ChannelID: m.ChannelID.Hex(), SenderID: m.SenderID, ClientMessageID: m.ClientMessageID, Type: m.Type, Content: m.Content, Sequence: m.Sequence, Revision: m.Revision, RecalledAt: m.RecalledAt, CreatedAt: m.CreatedAt, UpdatedAt: m.UpdatedAt}
	if !m.ReplyToMessageID.IsZero() {
		result.ReplyToMessageID = m.ReplyToMessageID.Hex()
	}
	if m.RecalledAt != nil {
		result.Content = ""
	}
	return result
}
