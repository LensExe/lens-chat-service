package websocket

import (
	"github.com/gin-gonic/gin"
	ws "github.com/gorilla/websocket"
	"go-app/internal/middleware"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSocketIsolationLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hub := NewHub()
	defer hub.Close()
	handler := NewHandler(hub, []string{"https://frontend.example"})
	router := gin.New()
	router.GET("/ws", func(c *gin.Context) {
		// Test-only principal injection, production router always uses JWT middleware.
		c.Set(middleware.ContextUserID, c.Query("user"))
		c.Set(middleware.ContextTenantID, c.Query("tenant"))
		expiry := time.Now().Add(time.Hour)
		if c.Query("expires") == "soon" {
			expiry = time.Now().Add(300 * time.Millisecond)
		}
		c.Set(middleware.ContextExpiresAt, expiry)
		handler.Connect(c)
	})
	server := httptest.NewServer(router)
	defer server.Close()
	address := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	dial := func(query string) *ws.Conn {
		t.Helper()
		conn, _, err := ws.DefaultDialer.Dial(address+query, http.Header{"Origin": []string{"https://frontend.example"}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	if conn, resp, err := ws.DefaultDialer.Dial(address, http.Header{"Origin": []string{"https://evil.example"}}); err == nil {
		conn.Close()
		t.Fatal("accepted foreign origin")
	} else if resp == nil || resp.StatusCode != 403 {
		t.Fatal("expected origin rejection")
	}
	a := dial("?user=a&tenant=one")
	b := dial("?user=b&tenant=one")
	other := dial("?user=a&tenant=two")
	// Upgrade completes just before registry insertion; synchronize on registry state.
	deadline := time.Now().Add(time.Second)
	for {
		hub.mu.Lock()
		count := len(hub.clients)
		hub.mu.Unlock()
		if count == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connections not registered")
		}
		time.Sleep(time.Millisecond)
	}
	hub.Notify("one", [2]string{"a", "b"}, "NEW_MESSAGE", map[string]string{"id": "message"})
	for _, conn := range []*ws.Conn{a, b} {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		var event Event
		if err := conn.ReadJSON(&event); err != nil {
			t.Fatal(err)
		}
		if event.Event != "NEW_MESSAGE" {
			t.Fatal("wrong event")
		}
	}
	other.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if _, _, err := other.ReadMessage(); err == nil {
		t.Fatal("cross-tenant event leak")
	}
	expiring := dial("?user=x&tenant=one&expires=soon")
	expiring.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := expiring.ReadMessage(); err == nil {
		t.Fatal("expired socket stayed open")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				hub.Notify("one", [2]string{"a", "b"}, "UPDATE", nil)
			}
		}()
	}
	a.Close()
	b.Close()
	hub.Close()
	wg.Wait()
	hub.Close()
}
