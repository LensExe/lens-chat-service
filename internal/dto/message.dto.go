package dto

import "time"

type CreateMessageRequest struct {
	ClientMessageID  string `json:"client_message_id" binding:"required,max=100"`
	Type             string `json:"type" binding:"required,oneof=text image file"`
	Content          string `json:"content" binding:"max=10000"`
	ReplyToMessageID string `json:"reply_to_message_id,omitempty"`
}

type UpdateMessageRequest struct {
	Content string `json:"content" binding:"required,max=10000"`
}

type AdvanceReadCursorRequest struct {
	LastReadSeq int64 `json:"last_read_seq" binding:"required,gte=1"`
}

type MessageResponse struct {
	ID               string     `json:"id"`
	ChannelID        string     `json:"channel_id"`
	SenderID         string     `json:"sender_id"`
	ClientMessageID  string     `json:"client_message_id"`
	Type             string     `json:"type"`
	Content          string     `json:"content"`
	Sequence         int64      `json:"seq"`
	ReplyToMessageID string     `json:"reply_to_message_id,omitempty"`
	Revision         int64      `json:"revision"`
	RecalledAt       *time.Time `json:"recalled_at,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}
