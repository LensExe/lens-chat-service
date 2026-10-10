package router

import (
	"go-app/internal/channel"

	"github.com/gin-gonic/gin"
)

func registerChannelRoutes(api *gin.RouterGroup, controller *channel.Controller) {
	api.POST("/channels/direct", controller.Create)
	api.GET("/channels", controller.List)
	api.GET("/channels/:channel-id", controller.Get)
}
