package router

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"go-app/internal/message"

	"github.com/gin-gonic/gin"
)

func registerAIMessageRoutes(r *gin.Engine, controller *message.Controller, token string) {
	internal := r.Group("/internal/v1/chat")
	internal.Use(aiWorkerAuth(token))
	internal.GET("/channels/:channel-id/messages", controller.ListForAI)
	internal.POST("/channels/:channel-id/ai-replies", controller.CreateAIReply)
}

func aiWorkerAuth(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if strings.TrimSpace(token) == "" {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"error": gin.H{"code": "AI_WORKER_DISABLED", "message": "AI worker integration is not configured"},
			})
			return
		}
		provided := c.GetHeader("X-AI-Worker-Token")
		if len(provided) != len(token) || subtle.ConstantTimeCompare([]byte(provided), []byte(token)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{"code": "UNAUTHORIZED", "message": "service credential is missing or invalid"},
			})
			return
		}
		c.Next()
	}
}
