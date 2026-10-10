package router

import (
	"context"
	"go-app/internal/channel"
	"go-app/internal/message"
	"go-app/internal/middleware"
	"net/http"

	"github.com/gin-gonic/gin"
)

func New(
	ch *channel.Controller,
	msg *message.Controller,
	verifier *middleware.KeycloakVerifier,
	origins []string, ping func(context.Context) error, metrics func(context.Context) (string, error),
) *gin.Engine {
	r := gin.New()
	registerCommonMiddleware(r, origins)
	registerHealthRoutes(r, ping, metrics)

	// The public contract is versioned as /api/v1/chat. Keep the previous
	// /v1/api prefix during the migration so existing clients do not break
	// while Kong and frontend clients move to the canonical path.
	publicAPI := r.Group("/api/v1/chat", verifier.GinMiddleware())
	registerChannelRoutes(publicAPI, ch)
	registerMessageRoutes(publicAPI, msg)

	legacyAPI := r.Group("/v1/api", verifier.GinMiddleware())
	registerChannelRoutes(legacyAPI, ch)
	registerMessageRoutes(legacyAPI, msg)

	return r
}

func registerCommonMiddleware(r *gin.Engine, origins []string) {
	r.Use(gin.Recovery(), middleware.CORS(origins))
	r.Use(func(c *gin.Context) { c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10); c.Next() })
}
