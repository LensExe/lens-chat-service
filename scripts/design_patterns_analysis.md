# 🎯 Design Pattern Recommendations for Go Chat App

## System Overview

Your system is a **real-time chat app** written in Go featuring:
- **Layered Architecture (Controller → Service → Repository)**
- **WebSocket** for real-time messaging
- **MongoDB** as the primary database
- **Redis** cache, **MinIO** storage
- **Dependency Injection** via Google Wire

---

## ✅ Well-Implemented Patterns (Keep as is)

| Pattern | Usage Location | Assessment |
|---------|----------------|------------|
| **Repository Pattern** | `user.repo.go`, `message/repo/` | ✅ Good — cleanly separates data access logic |
| **Dependency Injection** | `wire/wire_gen.go` | ✅ Good — using Google Wire |
| **Interface Segregation** | `IUserService`, `IMessageService`, `IWsHub` | ✅ Good — decouples components across layers |
| **Observer (manual)** | `Hub.Notify()` → loop broadcast | ⚠️ Functional, but can be improved |

---

## 🔴 Current Pain Points & Recommended Patterns

### 1. 🔁 Observer Pattern — Improve Real-Time Event Dispatching

**Current Issue:** In `message.controller.go`, every action (create, update, recall) must **manually loop** through channel members to broadcast:

```go
// Repeated across CreateMessage, UpdateMessage, RecallMessage...
memberIds := mc.messageService.GetMemberIds(msgDto.ChannelId)
for _, memberId := range memberIds {
    mc.hub.Notify(memberId, websocket.EventNewMessage, messageResponse)
}
```

**Drawbacks:** 
- Code duplication across multiple action handlers.
- Controller knows too much about WebSocket broadcasting details.
- Harder to test components in isolation.

**Solution — Event Bus (Observer Pattern):**

```go
// pkg/eventbus/eventbus.go
type Event struct {
    Type    string
    Payload interface{}
}

type Handler func(event Event)

type EventBus interface {
    Publish(eventType string, payload interface{})
    Subscribe(eventType string, handler Handler)
}

// When creating a message, service simply publishes:
s.eventBus.Publish("message.created", messageResponse)

// WebSocket Hub subscribes and handles broadcasting independently:
bus.Subscribe("message.created", func(e Event) {
    // broadcast to members
})
```

**Benefits:** Controller no longer needs to be aware of WebSocket details. Service simply emits events. Adding new features (push notifications, email alerts) requires zero changes to existing business logic.

---

### 2. 🔄 Mapper Pattern — Eliminate Mapping Code Duplication

**Current Issue:** Mapping from `schema.DbUser` → `dto.UserResponseDto` is **duplicated 4 times** in `user.service.go`:

```go
// Duplicated in GetUser, GetUserById, CreateUser, UpdateUser
userRes := dto.UserResponseDto{
    UserId:    result.ID.Hex(),
    UserName:  result.UserName,
    Email:     result.Email,
    AvatarUrl: result.AvatarUrl,
    IsActive:  &result.IsActive,
    Role:      result.Role.Hex(),
}
```

Similarly, `MessageResponseDto` mapping is duplicated in both `message.service.go` and `message.controller.go`.

**Solution — Dedicated Mapper Functions:**

```go
// internal/user/user.mapper.go
package user

func toUserResponseDto(u *schema.DbUser) *dto.UserResponseDto {
    if u == nil {
        return nil
    }
    return &dto.UserResponseDto{
        UserId:    u.ID.Hex(),
        UserName:  u.UserName,
        Email:     u.Email,
        AvatarUrl: u.AvatarUrl,
        IsActive:  &u.IsActive,
        Role:      u.Role.Hex(),
    }
}

// Used in service:
func (us *userService) GetUser(email string) *dto.UserResponseDto {
    return toUserResponseDto(us.userRepo.GetUser(email))
}
```

**Benefits:** Single place to update schemas, eliminating missed fields when adding new struct attributes.

---

### 3. 🏗️ Builder Pattern — Construct Complex MongoDB Queries & Filters

**Current Issue:** In `user.repo.go`, MongoDB filter construction logic is inline and scattered:

```go
filter := bson.M{
    "$or": []bson.M{
        {"user_name": bson.M{"$regex": keyword, "$options": "i"}},
        {"email": bson.M{"$regex": keyword, "$options": "i"}},
    },
}
if userID != primitive.NilObjectID {
    filter = bson.M{"$and": []bson.M{filter, {"_id": bson.M{"$ne": userID}}}}
}
```

As queries become more complex (adding role filters, online status filters, etc.), the repository code quickly becomes unreadable.

**Solution — Query Builder:**

```go
// internal/user/user.query.go
type UserQueryBuilder struct {
    filters []bson.M
}

func NewUserQuery() *UserQueryBuilder {
    return &UserQueryBuilder{}
}

func (b *UserQueryBuilder) WithKeyword(keyword string) *UserQueryBuilder {
    b.filters = append(b.filters, bson.M{
        "$or": []bson.M{
            {"user_name": bson.M{"$regex": keyword, "$options": "i"}},
            {"email": bson.M{"$regex": keyword, "$options": "i"}},
        },
    })
    return b
}

func (b *UserQueryBuilder) ExcludeUser(userId primitive.ObjectID) *UserQueryBuilder {
    if userId != primitive.NilObjectID {
        b.filters = append(b.filters, bson.M{"_id": bson.M{"$ne": userId}})
    }
    return b
}

func (b *UserQueryBuilder) Build() bson.M {
    if len(b.filters) == 0 {
        return bson.M{}
    }
    if len(b.filters) == 1 {
        return b.filters[0]
    }
    return bson.M{"$and": b.filters}
}

// Usage:
filter := NewUserQuery().
    WithKeyword(keyword).
    ExcludeUser(userID).
    Build()
```

---

### 4. 🔒 Command Pattern / Routing Map — Handle Inbound WebSocket Messages

**Current Issue:** In `client.go`/`handler.go`, receiving messages over WebSocket lacks structured validation, authentication, and dynamic routing to appropriate handlers.

**Solution:** As the system grows to support multiple WS event types, employ the **Command Pattern** + router mapping:

```go
// internal/websocket/commands.go
type WsCommand interface {
    Execute(client *Client, payload json.RawMessage) error
}

type WsRouter struct {
    handlers map[string]WsCommand
}

func (r *WsRouter) Register(event string, cmd WsCommand) {
    r.handlers[event] = cmd
}

func (r *WsRouter) Dispatch(client *Client, event string, payload json.RawMessage) error {
    cmd, ok := r.handlers[event]
    if !ok {
        return errors.New("unknown event: " + event)
    }
    return cmd.Execute(client, payload)
}
```

---

## 📊 Priority Matrix

| # | Pattern | Priority | Effort | Impact | Application Area |
|---|---------|-----------|--------|--------|------------------|
| 1 | **Mapper Pattern** | 🔴 High | 🟢 Low | 🟢 Immediate | `user.service.go`, `message.service.go` |
| 2 | **Observer / Event Bus** | 🟡 Medium | 🟡 Medium | 🔴 High | `message.controller.go` → decoupled real-time |
| 3 | **Builder Pattern** | 🟡 Medium | 🟢 Low | 🟡 Medium | `user.repo.go`, complex repository queries |
| 4 | **Command Pattern (WS)** | 🟢 Low | 🟡 Medium | 🟡 Medium | As new WS event types are introduced |

---

## 💡 Final Recommendations

> **Start with the Mapper Pattern** — Lowest risk, immediate code cleanup without major refactoring.
>
> **Next, adopt the Observer / Event Bus Pattern** — Significantly thins out Controllers and makes adding future notification systems seamless.

Your overall architecture (Repository, DI, Interfaces) is solid. Avoid applying too many patterns at once — focus directly on the areas causing the **highest friction**.
