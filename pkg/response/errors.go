package response

import (
	"errors"
	"github.com/gin-gonic/gin"
	"log/slog"
)

var (
	ErrInvalid   = errors.New("invalid request")
	ErrNotFound  = errors.New("resource not found")
	ErrForbidden = errors.New("operation not allowed")
	ErrConflict  = errors.New("conflicting request")
)

func FromError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, ErrInvalid):
		Fail(c, 400, "INVALID_REQUEST", err.Error())
	case errors.Is(err, ErrNotFound):
		Fail(c, 404, "NOT_FOUND", err.Error())
	case errors.Is(err, ErrForbidden):
		Fail(c, 403, "FORBIDDEN", err.Error())
	case errors.Is(err, ErrConflict):
		Fail(c, 409, "CONFLICT", err.Error())
	default:
		slog.Error("chat request failed", "error", err)
		Fail(c, 500, "INTERNAL_ERROR", "request failed")
	}
}
