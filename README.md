# Chat Service — Go/Gin, direct chat

Service chat 1-1, giữ cấu trúc modular controller → service → repository của dự án.
Identity lấy từ JWT Keycloak `sub` (chuỗi), không có database user hoặc API login/register.

## Cấu trúc

```text
cmd/server/main.go
internal/
  channel/                 # Direct channel controller/service
    repo/                  # Mongo channel repository, indexes
  message/                 # Message controller/service, read cursor
    repo/                  # Mongo message/state repository, transaction
  websocket/               # Local multi-device hub, connection lifecycle
  middleware/              # Keycloak JWT local verification, CORS
  router/                  # Gin routes
  initialize/              # Configuration, constructor wiring, lifecycle
  dto/                     # HTTP payloads
  schema/                  # Mongo chat documents
pkg/
  mapper/
  response/
  setting/
```

## Chạy local

1. Dùng `config.example.yaml` làm mẫu cho `config/local.yaml` hoặc dùng biến môi trường.
2. Cấu hình `KEYCLOAK_ISSUER`, `KEYCLOAK_AUDIENCE` và `KEYCLOAK_PUBLIC_KEY_FILE` hoặc `KEYCLOAK_PUBLIC_KEY`.
3. Chạy Mongo replica set: `docker compose -f deploy/docker-compose.infra.yaml up -d`.
4. `go run ./cmd/server`.

Mongo URI cho Go chạy trên host: `mongodb://localhost:27017/?replicaSet=rs0&directConnection=true`.
Backend container cần URI truy cập được từ container; trên Docker Desktop có thể dùng `host.docker.internal` với `directConnection=true`.
Compose infra chỉ dùng local; production cần replica set có authentication.

## Keycloak

- Public key RSA nạp một lần lúc khởi động từ PEM hoặc public key base64 của realm.
- Verify local bằng `golang-jwt/jwt/v5`; không gọi introspection, userinfo hay JWKS HTTP.
- Bắt buộc RS256, `iss`, `aud`, `exp`, `sub`, `typ=Bearer`; kiểm `nbf` khi có; lệch đồng hồ tối đa 30 giây.
- Cấu hình audience mapper của Keycloak để access token chứa audience `chat-service` (hoặc giá trị đã cấu hình).
- Khi rotate signing key: cập nhật public key rồi restart/rollout. Bản này không tự refresh JWKS.
- HTTP dùng `Authorization: Bearer <access-token>`.
- Claim `tenant_id` phân vùng dữ liệu; mặc định `default`. Không lấy tenant/actor từ body hoặc header tự khai.
- Browser WebSocket: `new WebSocket(url, ["access_token", token])`. Server chỉ echo subprotocol `access_token`. Gateway không log header `Sec-WebSocket-Protocol` hoặc `Authorization`.
- Socket đóng lúc token hết hạn; client lấy token mới ở hệ thống identity rồi reconnect. Không hỗ trợ `?token=`.

## API

Base path `/v1/api`. Actor luôn từ JWT.

| Method | Route | Payload/query |
|---|---|---|
| POST | `/channels/direct` | `{"peer_id":"keycloak-user-id"}`; get-or-create trả 200 |
| GET | `/channels` | Tối đa 100 channel gần nhất của actor |
| GET | `/channels/:channel-id` | Chỉ participant |
| POST | `/channels/:channel-id/messages` | `{"client_message_id":"unique-client-id","type":"text","content":"Hello"}` |
| GET | `/channels/:channel-id/messages` | `limit=20&before_seq=0`; seq giảm dần, limit 1–100 |
| PATCH | `/messages/:message-id` | `{"content":"Edited"}`; chỉ sender |
| POST | `/messages/:message-id/recall` | Chỉ sender; tombstone |
| PUT | `/channels/:channel-id/read-cursor` | `{"last_read_seq":12}`; chỉ actor |
| GET | `/ws` | WebSocket realtime |
| GET | `/livez`, `/readyz` | Health/readiness |

Response: `{"data":...}`; lỗi: `{"error":{"code":"...","message":"..."}}`.
Message mới trả 201; retry cùng `(tenant, channel, sender, client_message_id)` và payload trả 200; payload khác trả 409.
Reply dùng `reply_to_message_id` và chỉ được tham chiếu message trong cùng channel.
Text/image/file là nội dung hoặc reference do client cung cấp; service không upload/download/fetch URL. Binary được xử lý bên ngoài.
Service kiểm quyền theo hai participant, chưa kiểm peer có tồn tại trong Keycloak hay policy block/friend của service khác.

Realtime: `{"event_id":"uuid","event":"NEW_MESSAGE","payload":{...}}`.
Events: `NEW_MESSAGE`, `UPDATED_MESSAGE`, `RECALLED_MESSAGE`, `READ_CURSOR`.
Client dedupe bằng message ID, chỉ áp dụng revision mới hơn; refresh history khi reconnect để lấy lại edit/recall bị miss.

## Phạm vi và tương thích

- Một replica cho realtime; hub multi-device có mutex, deadline và loại socket chậm. Chưa triển khai Redis/Kafka/outbox/cross-replica fan-out.
- Mongo transaction tăng seq, lưu message và cập nhật channel summary cùng commit. Cần replica set. Realtime sau commit là best-effort, có thể miss khi crash; REST history là nguồn dữ liệu chính.
- Đã bỏ auth/user/role/refresh-token/admin/group/social connection/upload cũ và generated Swagger/Wire references. Giữ controller/service/repo, không thêm layer Domain/Usecase.
- API/schema có breaking changes. Dùng **database mới** (`chat_direct`) để tránh xung đột schema/index legacy. Chưa migration lịch sử cũ; không trỏ bản mới trực tiếp vào database cũ.
- Các tài liệu thiết kế cũ là roadmap tham khảo. Phạm vi cập nhật ở đầu `scripts/chat_microservice_refactor_design.md`.
- Không thêm dependency trực tiếp mới: giữ Gin/CORS, JWT, UUID, Gorilla WebSocket, Viper, Mongo v1. Mongo v2 còn xuất hiện gián tiếp qua Gin binding.

## Kiểm tra

```sh
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/server
```

Integration test tạo/xóa database test riêng, yêu cầu replica set:

```sh
CHAT_TEST_MONGO_URI='mongodb://localhost:27017/?replicaSet=rs0&directConnection=true' go test ./internal/message/repo -run TestMongo -count=1
```
