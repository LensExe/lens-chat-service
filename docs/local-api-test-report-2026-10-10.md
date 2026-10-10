# Báo cáo kiểm thử Chat Service local

**Ngày chạy/cập nhật:** 10/10/2026 (Asia/Ho_Chi_Minh)  
**Kết quả hiện tại trên Mongo standalone:** 69/69 API checks đạt; Mongo integration test, `go test ./...`, `go vet ./...` và Docker image build đều đạt.

## Môi trường

- Đã kiểm tra `../lens-backend/.docker/chat-service/compose.yaml`; `docker compose config` render thành công. Compose gốc cố định host port `27017`, nhưng cổng này đang do container `go-app-mongodb` sử dụng. Mình giữ nguyên container đó.
- Cấu hình `../lens-backend/.docker/chat-service/compose.yaml` đã được đổi sang `mongod --bind_ip_all` và healthcheck ping (không replica set); `docker compose config` hợp lệ. File đó vẫn publish host port `27017`, đang do `go-app-mongodb` sử dụng, nên mình giữ nguyên container đó.
- `../lens-backend/.docker/chat-service/docker-compose.backend.yaml` cũng đã bỏ `replicaSet` khỏi URI mặc định của Chat.
- Tạo/cập nhật [`docker-compose.local-test.yaml`](../docker-compose.local-test.yaml): Mongo `7.0.34` standalone (không `--replSet`), gắn `lens-network`, alias `chat-mongodb`, host port `127.0.0.1:27018`. `db.hello()` xác nhận `standalone`; `chat-service-test-mongodb` đang healthy. Volume mới là `chat-service-local-test_chat_service_standalone_data`.
- Chat Service chạy như tiến trình Go đơn trên host, kết nối Mongo bằng `mongodb://127.0.0.1:27018/` không có `replicaSet` hay `directConnection`. Smoke runner tự build binary vào thư mục tạm, chọn cổng HTTP rảnh và dừng service sau khi chạy.
- Runner sinh RSA key và JWT test tạm tại runtime; không gọi Keycloak thật. Mỗi lượt dùng Mongo database có tên ngẫu nhiên rồi xóa database khi kết thúc.
- Kafka không chạy trong môi trường này. Runner trỏ publisher vào `127.0.0.1:1` để kiểm tra HTTP/outbox khi broker vắng mặt; API vẫn trả thành công, còn log ghi nhận publish retry. Vì vậy lượt này **không xác nhận Kafka nhận event**.

## API đã gọi

| Nhóm | Request và kết quả |
|---|---|
| Health/metrics | `GET /livez` 200; `GET /readyz` 200; `GET /metrics` 200, trả Prometheus text |
| JWT | Thiếu bearer, chữ ký RSA sai và token hết hạn đều bị từ chối 401 |
| Direct channel | Tạo và get-or-create từ cả hai phía trả 200 cùng channel ID; self-chat, peer rỗng và JSON sai trả 400; list trả 200; participant xem được 200; outsider nhận 404; channel ID sai định dạng trả 400 |
| Prefix tương thích | `GET /v1/api/channels` trả 200 |
| Tạo message | Text mới trả 201, sequence 1/revision 1; retry cùng payload trả 200 cùng ID; cùng idempotency key với payload khác trả 409; type không hỗ trợ và content rỗng trả 400 |
| Image/file | Message `image` và `file` với nội dung là reference trả 201; type và reference được giữ nguyên |
| Reply/history | Reply trong cùng channel trả 201, sequence 2 và tham chiếu đúng message; reply sang channel khác trả 400; history trả 200 theo thứ tự sequence giảm dần; `before_seq` phân trang đúng; limit 0/101 và `before_seq` âm trả 400; outsider nhận 404 |
| Edit/recall | Sender edit trả 200, content đổi và revision tăng; người khác edit/recall trả 403; content edit rỗng trả 400; recall trả 200 và xóa content; recall lần nữa trả 200; edit sau recall trả 409; message ID sai định dạng trả 400 |
| Read cursor | Tiến tới sequence 2 trả 200; cập nhật lùi vẫn giữ sequence 2; sequence 0 hoặc vượt message mới nhất trả 400; outsider nhận 404 |

## Kiểm tra Go và image

- `CHAT_TEST_MONGO_URI='mongodb://127.0.0.1:27018/' go test ./... -count=1` — đạt. `TestMongoStandaloneWritesAndIdempotency` đã chạy trên Mongo standalone, không bị skip.
- `go vet ./...` — đạt, không có diagnostic.
- Dockerfile sẵn có build thành công bằng `docker build --tag chat-service:local-test .`.
- Image đã kiểm tra metadata: tag `chat-service:local-test`, kiến trúc `arm64`, kích thước 51,283,729 bytes (~51.3 MB). Dockerfile chưa cần thay đổi.

## Chạy lại

Từ thư mục gốc repo:

```sh
docker compose -f docker-compose.local-test.yaml up -d --wait
go run ./scripts/local-api-smoke
go test ./...
go vet ./...
docker build --tag chat-service:local-test .
```

Để dừng Mongo test, chạy `docker compose -f docker-compose.local-test.yaml down`. Lệnh này giữ volume; thêm `-v` sẽ xóa dữ liệu Mongo test.

## Ghi chú lần chạy

- Lượt khởi động Compose gốc thất bại do xung đột port `27017`; không container/dữ liệu cũ nào bị dừng hoặc sửa. Compose test riêng trên port `27018` đã chạy thành công.
- Volume rỗng `deploy_chat_direct_data` được Compose gốc tạo trước lỗi bind port; không được mount và vẫn được giữ nguyên. Volume replica-set cũ `chat-service-local-test_chat_service_local_test_mongo_data` cũng được giữ nguyên khi thay test container sang volume standalone mới.
- Standalone không hỗ trợ transaction đa document: nếu process dừng giữa lúc ghi domain state và outbox, event có thể bị thiếu hoặc aggregate version có thể có gap. Client cần đồng bộ bằng REST history. Outbox của các event đã insert vẫn retry/publish bình thường.
- Không reset các thay đổi khác đã có trong working tree. Các file mới cho quy trình này là Compose test, [`scripts/local-api-smoke/main.go`](../scripts/local-api-smoke/main.go) và báo cáo này.
