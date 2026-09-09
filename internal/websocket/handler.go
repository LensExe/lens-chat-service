package websocket

import (
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go-app/internal/middleware"
	"go-app/pkg/response"
	"net/http"
	"time"
)

type Handler struct {
	hub      *Hub
	upgrader websocket.Upgrader
}

func NewHandler(hub *Hub, origins []string) *Handler {
	allowed := make(map[string]bool)
	for _, origin := range origins {
		allowed[origin] = true
	}
	return &Handler{hub: hub, upgrader: websocket.Upgrader{Subprotocols: []string{"access_token"}, HandshakeTimeout: 5 * time.Second, CheckOrigin: func(r *http.Request) bool { origin := r.Header.Get("Origin"); return origin == "" || allowed[origin] }}}
}
func (h *Handler) Connect(c *gin.Context) {
	expiry, ok := c.Get(middleware.ContextExpiresAt)
	if !ok {
		response.Unauthorized(c)
		return
	}
	expires := expiry.(time.Time)
	if !time.Now().Before(expires) {
		response.Unauthorized(c)
		return
	}
	conn, err := h.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	client := &client{conn: conn, send: make(chan Event, 64), tenant: middleware.TenantID(c), user: middleware.UserID(c), expires: expires}
	if !h.hub.register(client) {
		conn.Close()
		return
	}
	go h.write(client)
	go h.read(client)
}
func (h *Handler) read(c *client) {
	defer h.hub.remove(c)
	c.conn.SetReadLimit(4096)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error { return c.conn.SetReadDeadline(time.Now().Add(60 * time.Second)) })
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}
func (h *Handler) write(c *client) {
	defer h.hub.remove(c)
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	expiration := time.NewTimer(time.Until(c.expires))
	defer expiration.Stop()
	for {
		select {
		case <-expiration.C:
			return
		case e, ok := <-c.send:
			if !ok || !time.Now().Before(c.expires) {
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if c.conn.WriteJSON(e) != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if c.conn.WriteMessage(websocket.PingMessage, nil) != nil {
				return
			}
		}
	}
}
