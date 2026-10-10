package router

import (
	"go-app/internal/message"

	"github.com/gin-gonic/gin"
)

func registerMessageRoutes(api *gin.RouterGroup, controller *message.Controller) {
	api.POST("/channels/:channel-id/messages", controller.Create)
	api.GET("/channels/:channel-id/messages", controller.List)
	api.PUT("/channels/:channel-id/read-cursor", controller.Read)
	api.PATCH("/messages/:message-id", controller.Edit)
	api.POST("/messages/:message-id/recall", controller.Recall)
}
