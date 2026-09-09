package message

import (
	"github.com/gin-gonic/gin"
	"go-app/internal/dto"
	"go-app/internal/middleware"
	"go-app/pkg/mapper"
	"go-app/pkg/response"
	"strconv"
)

type Controller struct{ service *Service }

func NewController(s *Service) *Controller { return &Controller{s} }
func (h *Controller) Create(c *gin.Context) {
	var req dto.CreateMessageRequest
	if c.ShouldBindJSON(&req) != nil {
		response.FromError(c, response.ErrInvalid)
		return
	}
	msg, created, err := h.service.Create(c.Request.Context(), middleware.TenantID(c), middleware.UserID(c), c.Param("channel-id"), req)
	if err != nil {
		response.FromError(c, err)
		return
	}
	status := 200
	if created {
		status = 201
	}
	response.OK(c, status, mapper.Message(msg))
}
func (h *Controller) List(c *gin.Context) {
	limit, e1 := strconv.ParseInt(c.DefaultQuery("limit", "20"), 10, 64)
	before, e2 := strconv.ParseInt(c.DefaultQuery("before_seq", "0"), 10, 64)
	if e1 != nil || e2 != nil {
		response.FromError(c, response.ErrInvalid)
		return
	}
	messages, err := h.service.List(c.Request.Context(), middleware.TenantID(c), middleware.UserID(c), c.Param("channel-id"), limit, before)
	if err != nil {
		response.FromError(c, err)
		return
	}
	result := make([]dto.MessageResponse, 0, len(messages))
	for i := range messages {
		result = append(result, mapper.Message(&messages[i]))
	}
	response.OK(c, 200, result)
}
func (h *Controller) Edit(c *gin.Context) {
	var req dto.UpdateMessageRequest
	if c.ShouldBindJSON(&req) != nil {
		response.FromError(c, response.ErrInvalid)
		return
	}
	msg, err := h.service.Edit(c.Request.Context(), middleware.TenantID(c), middleware.UserID(c), c.Param("message-id"), req.Content)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, 200, mapper.Message(msg))
}
func (h *Controller) Recall(c *gin.Context) {
	msg, err := h.service.Recall(c.Request.Context(), middleware.TenantID(c), middleware.UserID(c), c.Param("message-id"))
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, 200, mapper.Message(msg))
}
func (h *Controller) Read(c *gin.Context) {
	var req dto.AdvanceReadCursorRequest
	if c.ShouldBindJSON(&req) != nil {
		response.FromError(c, response.ErrInvalid)
		return
	}
	seq, err := h.service.Read(c.Request.Context(), middleware.TenantID(c), middleware.UserID(c), c.Param("channel-id"), req.LastReadSeq)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, 200, gin.H{"last_read_seq": seq})
}
