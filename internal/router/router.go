package router

import (
	"context"
	"github.com/gin-gonic/gin"
	"go-app/internal/channel"
	"go-app/internal/message"
	"go-app/internal/middleware"
	ws "go-app/internal/websocket"
	"net/http"
	"time"
)

func New(ch *channel.Controller, msg *message.Controller, socket *ws.Handler, verifier *middleware.KeycloakVerifier, origins []string, ping func(context.Context) error) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery(), middleware.CORS(origins))
	r.Use(func(c *gin.Context) { c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10); c.Next() })
	r.GET("/livez", func(c *gin.Context) { c.Status(200) })
	r.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if ping(ctx) != nil {
			c.Status(503)
			return
		}
		c.Status(200)
	})
	api := r.Group("/v1/api", verifier.GinMiddleware())
	api.POST("/channels/direct", ch.Create)
	api.GET("/channels", ch.List)
	api.GET("/channels/:channel-id", ch.Get)
	api.POST("/channels/:channel-id/messages", msg.Create)
	api.GET("/channels/:channel-id/messages", msg.List)
	api.PUT("/channels/:channel-id/read-cursor", msg.Read)
	api.PATCH("/messages/:message-id", msg.Edit)
	api.POST("/messages/:message-id/recall", msg.Recall)
	api.GET("/ws", socket.Connect)
	return r
}
