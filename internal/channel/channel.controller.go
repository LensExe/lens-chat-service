package channel

import (
	"github.com/gin-gonic/gin"
	"go-app/internal/dto"
	"go-app/internal/middleware"
	"go-app/pkg/mapper"
	"go-app/pkg/response"
)

type Controller struct{ service *Service }

func NewController(s *Service) *Controller { return &Controller{s} }
func (h *Controller) Create(c *gin.Context) {
	var req dto.CreateDirectChannelRequest
	if c.ShouldBindJSON(&req) != nil {
		response.FromError(c, response.ErrInvalid)
		return
	}
	ch, err := h.service.Create(c.Request.Context(), middleware.TenantID(c), middleware.UserID(c), req.PeerID)
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, 200, mapper.Channel(ch, middleware.UserID(c)))
}
func (h *Controller) List(c *gin.Context) {
	channels, err := h.service.List(c.Request.Context(), middleware.TenantID(c), middleware.UserID(c))
	if err != nil {
		response.FromError(c, err)
		return
	}
	result := make([]dto.ChannelResponse, 0, len(channels))
	for i := range channels {
		result = append(result, mapper.Channel(&channels[i], middleware.UserID(c)))
	}
	response.OK(c, 200, result)
}
func (h *Controller) Get(c *gin.Context) {
	ch, err := h.service.Get(c.Request.Context(), middleware.TenantID(c), middleware.UserID(c), c.Param("channel-id"))
	if err != nil {
		response.FromError(c, err)
		return
	}
	response.OK(c, 200, mapper.Channel(ch, middleware.UserID(c)))
}
