package dto

import "time"

type CreateDirectChannelRequest struct {
	PeerID string `json:"peer_id" binding:"required"`
}

type ChannelResponse struct {
	ID             string    `json:"id"`
	ParticipantIDs [2]string `json:"participant_ids"`
	PeerID         string    `json:"peer_id"`
	LastMessageID  string    `json:"last_message_id,omitempty"`
	LastMessageSeq int64     `json:"last_message_seq"`
	LastMessageAt  time.Time `json:"last_message_at,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
