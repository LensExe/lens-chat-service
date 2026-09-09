# 🚀 In-Depth Go Concurrency Learning Path via Your Project

> Instead of learning through abstract tutorials, you will **implement directly into your chat app** — each feature will teach you a specific concept.

---

## Big Picture: Where Are You Now?

```
Your current codebase structure:
┌─────────────────────────────────────────────────────┐
│  Hub.Run() runs a single goroutine                   │
│  → select { case <-register / case <-unregister }   │
│                                                     │
│  Each Client has 2 goroutines:                      │
│  → ReadPump() goroutine (reads from WS)             │
│  → WritePump() goroutine (writes to WS)             │
│  │                                                  │
│  Hub.clients = map[string]map[string]*Client        │
│  (No mutex needed → safe because only 1 goroutine   │
│   reads/writes to the map)                          │
└─────────────────────────────────────────────────────┘
```

You have **intentionally or unintentionally** implemented the safest pattern in Go: **goroutine ownership**. Now, let's understand WHY and dive deeper into advanced concurrency patterns.

---

## 🎯 Level 1: Goroutine — Typing Indicator ("User is typing...")

**Concepts learned:** `goroutine`, `time.AfterFunc`, `defer`, goroutine lifecycle

**Feature:** When a user types, send a `TYPING_START` event over WS → server broadcasts it to channel members → automatically turns off after 3 seconds of inactivity.

### Why is this complex?
If a user types continuously, every keystroke should reset the 3-second timer. You need a goroutine to manage the timer and cancel the previous timer goroutine cleanly.

### Code to add to `hub.go`:

```go
// Concept: Goroutine with self-canceling timer
type typingState struct {
    cancel chan struct{} // Channel to signal "cancel this goroutine"
}

type Hub struct {
    clients    map[string]map[string]*Client
    register   chan *Client
    unregister chan *Client
    
    // NEW: map[channelId][userId] → timer goroutine state
    typingUsers map[string]map[string]*typingState // ← Learning goroutine ownership
}

// Upon receiving a TYPING_START event:
func (h *Hub) handleTyping(channelId, userId string) {
    // If an existing timer goroutine exists → cancel it first
    if state, ok := h.typingUsers[channelId][userId]; ok {
        close(state.cancel) // ← Signal the previous goroutine to stop
    }
    
    cancelCh := make(chan struct{})
    h.typingUsers[channelId][userId] = &typingState{cancel: cancelCh}
    
    // Spawn a new goroutine — automatically stops after 3 seconds or when canceled
    go func() {
        select {
        case <-time.After(3 * time.Second): // Timer expired
            h.stopTyping <- stopTypingEvent{channelId, userId}
        case <-cancelCh: // Canceled by a new keystroke
            return
        }
    }()
}
```

**Key Takeaways:**
- `close(ch)` is the Go idiomatic way to signal "stop goroutine" — never use `kill` style mechanisms.
- `select` with multiple cases allows a goroutine to wait on whichever event occurs first.
- Goroutines automatically exit when their function returns — no manual memory cleanup required.

---

## 🎯 Level 2: Channel — Message Rate Limiter

**Concepts learned:** Buffered channel, channel as a semaphore, `select` with `default`

**Feature:** Restrict each user to a maximum of 5 messages per second to prevent spam.

### Why use a channel instead of Mutex + Counter?

```go
// ❌ Naive approach (requires mutex, subject to race conditions):
type RateLimiter struct {
    mu    sync.Mutex
    count int
    reset time.Time
}

// ✅ Idiomatic Go approach — using a buffered channel as a "token bucket":
type RateLimiter struct {
    tokens chan struct{} // Buffered channel = "token bucket"
}

func NewRateLimiter(rate int) *RateLimiter {
    rl := &RateLimiter{
        tokens: make(chan struct{}, rate), // Capacity = max token limit
    }
    
    // Background goroutine continuously refilling tokens into the bucket
    go func() {
        ticker := time.NewTicker(time.Second / time.Duration(rate))
        defer ticker.Stop()
        for range ticker.C {
            select {
            case rl.tokens <- struct{}{}: // Replenish token (if bucket not full)
            default: // Bucket full, discard
            }
        }
    }()
    
    return rl
}

func (rl *RateLimiter) Allow() bool {
    select {
    case <-rl.tokens: // Consume 1 token
        return true
    default: // No tokens available → throttled
        return false
    }
}
```

**Usage in `client.go` → `ReadPump()`:**
```go
func (c *Client) ReadPump() {
    limiter := NewRateLimiter(5) // 5 messages/second
    
    for {
        var payload ClientMessagePayload
        c.Conn.ReadJSON(&payload)
        
        if !limiter.Allow() {
            c.Send <- WsResponse{Event: "RATE_LIMITED", Payload: "Too fast!"}
            continue
        }
        // Process message normally...
    }
}
```

**Key Takeaways:**
- Buffered channel `make(chan T, n)` — sends do not block as long as buffer capacity is available.
- `select` with `default` — enables non-blocking operations (checks immediately without waiting).
- Using a channel as a **token bucket** is a classic pattern in concurrent Go development.

---

## 🎯 Level 3: Mutex — Thread-Safe Hub Stats

**Concepts learned:** `sync.RWMutex`, when to use mutex vs channel, data race detection

**Real-world Problem:** Currently, `Hub.clients` is accessed solely by one goroutine (`Hub.Run()`) → perfectly thread-safe. However, if you want to expose an **HTTP endpoint** `/api/stats` to report online user count, reading `Hub.clients` from an HTTP handler goroutine introduces a **data race!**

```go
// ❌ Bug: Reading map outside of the Hub.Run() goroutine
func (h *Hub) GetOnlineCount() int {
    return len(h.clients) // DATA RACE! Another goroutine is modifying this map
}

// Running: go test -race → will immediately flag this issue!
```

### Fixing with `RWMutex`:

```go
type HubStats struct {
    mu          sync.RWMutex // RWMutex: multiple readers, 1 writer
    onlineCount int
    totalMsgs   int64
}

// Hub.Run() updates stats during register/unregister:
func (h *Hub) registerClient(client *Client) {
    // ... existing logic ...
    
    h.stats.mu.Lock()         // ← Exclusive writer lock
    h.stats.onlineCount++
    h.stats.mu.Unlock()
}

// HTTP handler reads stats (without blocking Hub.Run()):
func (h *Hub) GetStats() map[string]interface{} {
    h.stats.mu.RLock()        // ← Shared reader lock (allows concurrent readers)
    defer h.stats.mu.RUnlock()
    return map[string]interface{}{
        "online": h.stats.onlineCount,
        "msgs":   h.stats.totalMsgs,
    }
}
```

**Key Takeaways:**
- `sync.Mutex` → Only 1 goroutine permitted at a time (read or write).
- `sync.RWMutex` → Multiple concurrent readers permitted, but only 1 writer (much better read performance).
- Always use `defer mu.Unlock()` / `defer mu.RUnlock()` to prevent deadlocks during panics.
- Use Go runtime's `-race` flag for data race detection.

---

## 🎯 Level 4: Channel Pipeline — Parallel Message Processing

**Concepts learned:** Fan-out pattern, Pipeline pattern, `sync.WaitGroup`

**Feature:** When a message is sent to a group channel, the server needs to:
1. Save to Database (slow IO)
2. Send Push Notifications (slow network call)
3. Broadcast over WebSocket (fast)

**Executing sequentially = Slow. Executing in parallel = Fast (requires synchronization):**

```go
// Pipeline: Save to DB → Parallel [Push Notification + WS Broadcast]
func (h *Hub) ProcessNewMessage(msg MessageEvent) {
    // Stage 1: Save to DB (must complete first)
    savedMsg := saveToDatabase(msg) // blocking
    
    // Stage 2: Fan-out — 2 goroutines executing in parallel
    var wg sync.WaitGroup
    wg.Add(2)
    
    go func() {
        defer wg.Done()
        sendPushNotification(savedMsg) // Slow network call, does not block WS flow
    }()
    
    go func() {
        defer wg.Done()
        h.broadcastToChannel(savedMsg) // Fast WS broadcast
    }()
    
    wg.Wait() // Block until both goroutines finish
    // → Total execution time = max(push, broadcast), instead of push + broadcast
}
```

**Key Takeaways:**
- `sync.WaitGroup` — Tracks running goroutines and waits for all of them to signal completion.
- **Fan-out pattern** — 1 input event distributed across multiple parallel worker goroutines.
- **Pipeline pattern** — Chained goroutine stages connected via channels.

---

## 🎯 Level 5 (Boss): Context — Graceful Shutdown

**Concepts learned:** `context.Context`, `context.WithCancel`, `context.WithTimeout`, cancellation propagation

**Feature:** When the server receives a shutdown signal (Ctrl+C):
1. Stop accepting new connections.
2. Send a `"SERVER_SHUTDOWN"` message to all connected clients.
3. Wait up to 10 seconds for existing goroutines to clean up resources.
4. Force shutdown after the 10-second deadline.

```go
// Hub.Run() accepts context to listen for shutdown signals:
func (h *Hub) Run(ctx context.Context) {
    for {
        select {
        case client := <-h.register:
            h.registerClient(client)
            
        case client := <-h.unregister:
            h.unregisterClient(client)
            
        case <-ctx.Done(): // ← Received shutdown signal!
            // Broadcast shutdown message to all connected clients
            for userId, connections := range h.clients {
                for _, client := range connections {
                    client.Send <- WsResponse{
                        Event:   "SERVER_SHUTDOWN",
                        Payload: "Server is restarting...",
                    }
                }
                _ = userId
            }
            return // Exit Hub.Run() goroutine cleanly
        }
    }
}

// Client ReadPump/WritePump also accept context:
func (c *Client) WritePump(ctx context.Context) {
    defer c.Conn.Close()
    
    for {
        select {
        case res, ok := <-c.Send:
            if !ok {
                return // Channel closed
            }
            c.Conn.WriteJSON(res)
            
        case <-ctx.Done(): // ← Shutdown signal propagated down
            c.Conn.WriteMessage(websocket.CloseMessage, 
                websocket.FormatCloseMessage(1001, "Server going down"))
            return
        }
    }
}

// main.go:
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    
    go hub.Run(ctx)
    
    // Listen for OS interrupt signals (Ctrl+C)
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit // Block until signal received
    
    fmt.Println("Shutting down...")
    cancel() // Cancel context → propagates to all child goroutines
    
    // Wait for maximum of 10 seconds
    shutdownCtx, _ := context.WithTimeout(context.Background(), 10*time.Second)
    server.Shutdown(shutdownCtx)
}
```

**Key Takeaways:**
- `context.Context` — Standard mechanism for propagating cancellation down a goroutine tree.
- `context.WithCancel` — Creates a context that can be manually canceled.
- `context.WithTimeout` — Creates a context that cancels automatically after a duration.
- Graceful Shutdown is a standard requirement for production-grade Go services.

---

## 📋 Learning Path Summary

```
Level 1: Goroutine Lifecycle     → Typing Indicator
         ↓ (goroutine, defer, timer)
Level 2: Channel Semantics       → Rate Limiter
         ↓ (buffered channel, select/default, token bucket)
Level 3: Mutex & Data Race       → Hub Stats API
         ↓ (RWMutex, race detector, goroutine safety)
Level 4: Channel Pipeline        → Parallel Message Processing
         ↓ (fan-out, WaitGroup, pipeline pattern)
Level 5: Context Propagation     → Graceful Shutdown
         ↓ (context tree, cancellation, OS signals)
```

| Level | Feature | Core Concept | Impacted Files |
|-------|---------|--------------|----------------|
| 1 | Typing Indicator | goroutine + timer cancellation | `hub.go`, `types.go` |
| 2 | Rate Limiter | buffered channel, non-blocking select | `client.go` |
| 3 | Stats API | RWMutex, race conditions | `hub.go`, new HTTP route |
| 4 | Parallel Processing | WaitGroup, fan-out pattern | `message.controller.go` |
| 5 | Graceful Shutdown | context cancellation tree | `hub.go`, `client.go`, `main.go` |

---

## 🛠️ Essential Concurrency Diagnostic Tools

```bash
# Detect data races during testing and execution
go test -race ./...
go run -race main.go

# Inspect active goroutine stacks during deadlocks:
# Press Ctrl+\ while running in terminal to dump goroutine stacks

# Profile goroutines with pprof
import _ "net/http/pprof"
# Navigate to → http://localhost:6060/debug/pprof/goroutine?debug=1
```

> **Pro Tip:** Implement sequentially from Level 1 → 5. Each level builds on concepts learned in previous levels. After Level 3, you will fully understand why your current `Hub.Run()` loop is safe, and know precisely when mutexes or channels are required.
