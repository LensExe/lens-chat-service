package websocket

import (
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"sync"
	"time"
)

type Event struct {
	ID      string `json:"event_id"`
	Event   string `json:"event"`
	Payload any    `json:"payload"`
}
type client struct {
	conn         *websocket.Conn
	send         chan Event
	tenant, user string
	expires      time.Time
}
type Hub struct {
	mu      sync.Mutex
	clients map[*client]struct{}
	closed  bool
}

func NewHub() *Hub { return &Hub{clients: make(map[*client]struct{})} }
func (h *Hub) register(c *client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.clients[c] = struct{}{}
	return true
}
func (h *Hub) removeLocked(c *client) {
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
		c.conn.Close()
	}
}
func (h *Hub) remove(c *client) { h.mu.Lock(); defer h.mu.Unlock(); h.removeLocked(c) }

// Notify is best-effort delivery within this process. REST history remains authoritative.
func (h *Hub) Notify(tenant string, users [2]string, event string, payload any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := Event{ID: uuid.NewString(), Event: event, Payload: payload}
	for c := range h.clients {
		if c.tenant != tenant || (c.user != users[0] && c.user != users[1]) {
			continue
		}
		if !time.Now().Before(c.expires) {
			h.removeLocked(c)
			continue
		}
		select {
		case c.send <- e:
		default:
			h.removeLocked(c)
		}
	}
}
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for c := range h.clients {
		h.removeLocked(c)
	}
}
