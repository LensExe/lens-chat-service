package router

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func registerHealthRoutes(r *gin.Engine, ping func(context.Context) error, metrics func(context.Context) (string, error)) {
	r.GET("/livez", func(c *gin.Context) { c.Status(http.StatusOK) })
	r.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if ping(ctx) != nil {
			c.Status(http.StatusServiceUnavailable)
			return
		}
		c.Status(http.StatusOK)
	})
	r.GET("/metrics", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		body, err := metrics(ctx)
		if err != nil {
			c.Status(http.StatusServiceUnavailable)
			return
		}
		c.Data(http.StatusOK, "text/plain; version=0.0.4; charset=utf-8", []byte(body))
	})
}
