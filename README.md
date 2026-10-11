# Chat Service — Go/Gin, direct chat

Service chat 1-1, giữ cấu trúc modular controller → service → repository của dự án.
Identity lấy từ JWT Keycloak `sub` (chuỗi), không có database user hoặc API login/register.
Chat chạy trong một phạm vi dữ liệu duy nhất cho nền tảng booking. Channel lưu hai user ID lấy từ core/identity; Chat không sao chép hồ sơ user và không có collection participants riêng.

## Cấu trúc

```text
cmd/server/main.go
internal/
  channel/                 # Direct channel controller/service
    repo/                  # Mongo channel repository, indexes
  message/                 # Message controller/service, read cursor
    repo/                  # Mongo message/state repository, standalone-compatible writes
  outbox/                  # Mongo outbox repository, Kafka publisher and relay
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
3. Tạo network dùng chung từ thư mục `lens-backend` bằng `pnpm docker:network`, rồi chạy standalone Mongo: `docker compose -f docker-compose.local-test.yaml up -d --wait`.
4. `go run ./cmd/server`.

Chat dùng Kafka để phát domain event. Nếu Kafka chưa chạy, các event đã ghi vào outbox vẫn nằm trong Mongo và được relay thử tối đa 3 lần; nếu broker vẫn lỗi, event được giữ ở `outbox_events` với `status: "failed"` để operator kiểm tra/replay. Tạo topic `chat.conversation.events.v1` trước khi bật producer trong môi trường có tắt auto-create topic.

Các biến môi trường có thể dùng để kết nối broker:

```dotenv
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=chat-service
KAFKA_CHAT_EVENTS_TOPIC=chat.conversation.events.v1
KAFKA_USERNAME=kafka
KAFKA_PASSWORD=your-secret
KAFKA_SSL=false
KAFKA_OUTBOX_POLL_INTERVAL=1s
KAFKA_OUTBOX_LEASE_DURATION=30s
KAFKA_PUBLISH_TIMEOUT=10s
```

Username/password là tuỳ chọn nhưng phải cấu hình cùng nhau. Container Chat gắn vào `lens-network` dùng `KAFKA_BROKERS=kafka:9094` và bỏ username/password vì listener nội bộ dùng PLAINTEXT. Đặt secret trong environment/secret manager, không ghi vào file cấu hình được commit.

Mongo URI cho Go chạy trên host: `mongodb://localhost:27018/`.
Container tham gia `lens-network` dùng `mongodb://chat-mongodb:27017/`.
Compose Mongo này là standalone, không có replication/failover. Bật authentication và chọn topology phù hợp trước khi đưa database ra môi trường dùng chung hoặc production.

## Keycloak

- Public key RSA nạp một lần lúc khởi động từ PEM hoặc public key base64 của realm.
- Verify local bằng `golang-jwt/jwt/v5`; không gọi introspection, userinfo hay JWKS HTTP.
- Bắt buộc RS256, `iss`, `aud`, `exp`, `sub`, `typ=Bearer`; kiểm `nbf` khi có; lệch đồng hồ tối đa 30 giây.
- Cấu hình audience mapper của Keycloak để access token chứa audience `chat-service` (hoặc giá trị đã cấu hình).
- Khi rotate signing key: cập nhật public key rồi restart/rollout. Bản này không tự refresh JWKS.
- HTTP dùng `Authorization: Bearer <access-token>`.
- Actor luôn lấy từ JWT `sub`; dữ liệu và quyền truy cập được giới hạn theo user ID và tư cách participant trong channel.
- Realtime client kết nối Socket.IO trực tiếp tới Notification Service; Chat Service chỉ cung cấp HTTP API và Kafka domain events.

## API

Base path công khai qua Kong là `/api/v1/chat`. Actor luôn lấy từ JWT.

| Method | Route | Payload/query |
|---|---|---|
| POST | `/channels/direct` | `{"peer_id":"core-user-id"}`; get-or-create một channel dùng chung cho cặp photographer/customer, trả 200 |
| GET | `/channels` | Tối đa 100 channel gần nhất của actor |
| GET | `/channels/:channel-id` | Chỉ participant |
| POST | `/channels/:channel-id/messages` | `{"client_message_id":"unique-client-id","type":"text","content":"Hello"}` |
| GET | `/channels/:channel-id/messages` | `limit=20&before_seq=0`; seq giảm dần, limit 1–100 |
| PATCH | `/messages/:message-id` | `{"content":"Edited"}`; chỉ sender |
| POST | `/messages/:message-id/recall` | Chỉ sender; tombstone |
| PUT | `/channels/:channel-id/read-cursor` | `{"last_read_seq":12}`; chỉ actor |
| GET | `/livez`, `/readyz`, `/metrics` | Health/readiness and Prometheus metrics |

Response: `{"data":...}`; lỗi: `{"error":{"code":"...","message":"..."}}`.
Message mới trả 201; retry cùng `(channel, sender, client_message_id)` và payload trả 200; payload khác trả 409.
Reply dùng `reply_to_message_id` và chỉ được tham chiếu message trong cùng channel.
Text/image/file là nội dung hoặc reference do client cung cấp; service không upload/download/fetch URL. Binary được xử lý bên ngoài.
Service kiểm quyền theo hai participant, chưa kiểm peer có tồn tại trong Keycloak hay policy block/friend của service khác.

Các event `NEW_MESSAGE`, `UPDATED_MESSAGE`, `RECALLED_MESSAGE` và `READ_CURSOR` được ghi vào Mongo outbox sau thay đổi nghiệp vụ, rồi relay publish lên Kafka topic `chat.conversation.events.v1` với key `channel_id`. `aggregate_version` được tăng atomically trên document channel và client dùng nó phát hiện/lọc event đến trễ.

Chat Service không mở WebSocket và không phát realtime trong process. Client kết nối Socket.IO namespace `/notification` của Notification Service, nhận các event trên một transport thống nhất. Payload message dùng cùng shape với REST `MessageResponse` (`id`, `channel_id`, `sender_id`, `client_message_id`, `type`, `content`, `seq`, `revision`, timestamps và reply/recall fields). User offline hoặc bị mất event tải history từ Chat API rồi đồng bộ theo `seq`.

Chi tiết Kafka consumer, Redis Streams, retry/DLQ, duplicate và runbook khôi phục nằm trong [notification-system/md/chat-realtime-operations.md](../notification-system/md/chat-realtime-operations.md).

Relay giao at-least-once cho các event đã có trong outbox. Client dedupe bằng `event_id` (và message `id` như lớp bảo vệ bổ sung), chỉ áp dụng `revision` mới hơn; event có `aggregate_version` thấp hơn version cuối đã áp dụng sẽ bị bỏ qua. Standalone Mongo không bảo đảm event tồn tại nếu process dừng trước khi insert outbox. Nếu có gap version/sequence, tải lại trạng thái từ Chat API.

## Phạm vi và tương thích

- Chat outbox dùng một relay leader có thể publish tối đa 16 channel-head song song; mỗi channel chỉ xử lý event có `aggregate_version` kế tiếp. Lỗi publish được thử tối đa 3 lần tổng cộng; event lỗi được giữ ở trạng thái `failed` trong Mongo với payload và nguyên nhân để operator replay, còn channel khác tiếp tục. Kafka/outbox vẫn at-least-once.
- Mongo chạy standalone được; code dùng atomic update trên từng document và không mở transaction. Thay đổi business, channel summary và outbox là các thao tác riêng. Nếu write sau lỗi hoặc process dừng giữa các thao tác, request có thể trả lỗi dù message/read cursor đã đổi, hoặc message có thể không có event outbox (event version cũng có thể bị khuyết). Hãy kiểm tra REST history trước khi retry. Bản standalone này không cung cấp atomic outbox guarantee. Kafka relay chỉ publish event sau khi event đã được ghi vào outbox; message API không chờ Notification Service.
- Socket.IO và Redis backplane chỉ thuộc Notification Service; Chat Service không có Hub, WebSocket route hoặc delivery logic. REST history vẫn là nguồn dữ liệu chính để khôi phục event bị thiếu.
- `/metrics` xuất outbox pending/failed, tuổi event pending, publish attempts/failures/retries/parked và thời gian xử lý. Endpoint này không qua JWT middleware; chỉ expose trên mạng nội bộ và giới hạn quyền scrape.
- Đã bỏ auth/user/role/refresh-token/admin/group/social connection/upload cũ và generated Swagger/Wire references. Giữ controller/service/repo, không thêm layer Domain/Usecase.
- API/schema có breaking changes. Dùng **database mới** (`chat_direct`) để tránh xung đột schema/index legacy. Service không tự chuyển documents hoặc indexes cũ; migration lịch sử chưa được triển khai. Không trỏ bản mới trực tiếp vào database cũ.
- Các tài liệu thiết kế cũ là roadmap tham khảo. Phạm vi cập nhật ở đầu `scripts/chat_microservice_refactor_design.md`.
- Kafka producer dùng `segmentio/kafka-go`; các dependency hiện có gồm Gin/CORS, JWT, UUID, Viper và Mongo v1. Mongo v2 còn xuất hiện gián tiếp qua Gin binding.

## Kiểm tra

```sh
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/server
```

Integration test tạo/xóa database test riêng, chạy được với Mongo standalone:

```sh
CHAT_TEST_MONGO_URI='mongodb://localhost:27018/' go test ./internal/message/repo -run TestMongo -count=1
```
