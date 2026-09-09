package response

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func OK(c *gin.Context, status int, data any) {
	c.JSON(status, gin.H{"data": data})
}

func Fail(c *gin.Context, status int, code, message string) {
	c.JSON(status, gin.H{"error": Error{Code: code, Message: message}})
}

func Unauthorized(c *gin.Context) {
	Fail(c, http.StatusUnauthorized, "UNAUTHORIZED", "access token is missing or invalid")
}
