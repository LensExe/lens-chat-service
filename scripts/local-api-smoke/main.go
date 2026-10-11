package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const mongoURI = "mongodb://127.0.0.1:27018/"

var passed atomic.Int32
var failed atomic.Int32

func main() {
	database := fmt.Sprintf("chat_api_smoke_%d", time.Now().UnixNano())
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		fatal("generate temporary test key", err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		fatal("encode temporary test public key", err)
	}
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}))

	binary, err := buildServerBinary()
	if err != nil {
		fatal("build chat-service binary", err)
	}
	defer os.Remove(binary)

	port, err := availablePort()
	if err != nil {
		fatal("reserve local HTTP port", err)
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := exec.Command(binary)
	cmd.Env = withEnv(map[string]string{
		"SERVER_ADDRESS":              fmt.Sprintf("127.0.0.1:%d", port),
		"SERVER_MODE":                 "release",
		"MONGO_URI":                   mongoURI,
		"MONGO_DATABASE":              database,
		"KEYCLOAK_ISSUER":             "https://smoke-test.invalid/realms/chat",
		"KEYCLOAK_AUDIENCE":           "chat-service",
		"KEYCLOAK_PUBLIC_KEY":         publicPEM,
		"CORS_ALLOW_ORIGINS":          "http://localhost:3000",
		"KAFKA_BROKERS":               "127.0.0.1:1",
		"KAFKA_CLIENT_ID":             "chat-service-local-api-smoke",
		"KAFKA_CHAT_EVENTS_TOPIC":     "chat.conversation.events.v1",
		"KAFKA_OUTBOX_POLL_INTERVAL":  "100ms",
		"KAFKA_OUTBOX_LEASE_DURATION": "1s",
		"KAFKA_PUBLISH_TIMEOUT":       "250ms",
	})
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fatal("start chat-service binary", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	serverStopped := false
	defer func() {
		if !serverStopped {
			stopServer(cmd, done)
		}
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	if !waitForServer(client, baseURL+"/livez", done, 25*time.Second) {
		fmt.Println("FAIL service startup: server did not become live within 25s")
		failed.Add(1)
		return
	}

	actor := "smoke-customer-01"
	peer := "smoke-photographer-02"
	outsider := "smoke-outsider-03"
	actorToken := makeToken(key, actor, false)
	peerToken := makeToken(key, peer, false)
	outsiderToken := makeToken(key, outsider, false)
	wrongKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	wrongSignatureToken := makeToken(wrongKey, actor, false)
	expiredToken := makeToken(key, actor, true)
	api := &smokeAPI{client: client, base: baseURL}

	api.request("live health", http.MethodGet, "/livez", "", nil, http.StatusOK)
	api.request("Mongo readiness", http.MethodGet, "/readyz", "", nil, http.StatusOK)
	api.request("Prometheus metrics", http.MethodGet, "/metrics", "", nil, http.StatusOK)
	api.check("metrics content type is Prometheus text", strings.HasPrefix(api.lastContentType, "text/plain"))
	api.check("metrics response body present", len(api.lastBody) > 0)
	api.request("missing bearer token rejected", http.MethodGet, "/api/v1/chat/channels", "", nil, http.StatusUnauthorized)
	api.request("invalid signature rejected", http.MethodGet, "/api/v1/chat/channels", wrongSignatureToken, nil, http.StatusUnauthorized)
	api.request("expired token rejected", http.MethodGet, "/api/v1/chat/channels", expiredToken, nil, http.StatusUnauthorized)

	channelBody := api.request("create direct channel", http.MethodPost, "/api/v1/chat/channels/direct", actorToken, map[string]any{"peer_id": peer}, http.StatusOK)
	channelData := dataMap(channelBody)
	channelID := stringField(channelData, "id")
	api.check("channel id is a Mongo ObjectID", len(channelID) == 24)
	participants, ok := channelData["participant_ids"].([]any)
	api.check("channel contains both participants", ok && len(participants) == 2)
	if ok && len(participants) == 2 {
		api.check("participants are canonicalized", participants[0] == actor && participants[1] == peer)
	}
	if channelID == "" {
		fmt.Println("Channel ID missing; dependent endpoint checks will fail independently.")
	}

	reusedBody := api.request("direct channel is idempotent across participant order", http.MethodPost, "/api/v1/chat/channels/direct", peerToken, map[string]any{"peer_id": actor}, http.StatusOK)
	api.check("both users resolve to the same direct channel", channelID != "" && stringField(dataMap(reusedBody), "id") == channelID)
	api.request("self-chat rejected", http.MethodPost, "/api/v1/chat/channels/direct", actorToken, map[string]any{"peer_id": actor}, http.StatusBadRequest)
	api.request("blank peer rejected", http.MethodPost, "/api/v1/chat/channels/direct", actorToken, map[string]any{"peer_id": "   "}, http.StatusBadRequest)
	api.request("invalid channel JSON rejected", http.MethodPost, "/api/v1/chat/channels/direct", actorToken, "{", http.StatusBadRequest)

	listed := api.request("list actor channels", http.MethodGet, "/api/v1/chat/channels", actorToken, nil, http.StatusOK)
	listData, listOK := listed["data"].([]any)
	api.check("actor sees one direct channel", listOK && len(listData) == 1)
	api.request("get channel as participant", http.MethodGet, "/api/v1/chat/channels/"+channelID, actorToken, nil, http.StatusOK)
	api.request("channel hidden from outsider", http.MethodGet, "/api/v1/chat/channels/"+channelID, outsiderToken, nil, http.StatusNotFound)
	api.request("malformed channel id rejected", http.MethodGet, "/api/v1/chat/channels/not-an-object-id", actorToken, nil, http.StatusBadRequest)

	createMessage := map[string]any{"client_message_id": "client-smoke-001", "type": "text", "content": "hello from local API smoke test"}
	messageBody := api.request("create text message", http.MethodPost, "/api/v1/chat/channels/"+channelID+"/messages", actorToken, createMessage, http.StatusCreated)
	messageData := dataMap(messageBody)
	messageID := stringField(messageData, "id")
	api.check("new message has sequence 1", numberField(messageData, "seq") == 1)
	api.check("new message has revision 1", numberField(messageData, "revision") == 1)
	replay := api.request("same message request is idempotent", http.MethodPost, "/api/v1/chat/channels/"+channelID+"/messages", actorToken, createMessage, http.StatusOK)
	api.check("idempotent retry returns same message id", messageID != "" && stringField(dataMap(replay), "id") == messageID)
	conflictPayload := map[string]any{"client_message_id": "client-smoke-001", "type": "text", "content": "changed payload"}
	api.request("idempotency key with changed payload conflicts", http.MethodPost, "/api/v1/chat/channels/"+channelID+"/messages", actorToken, conflictPayload, http.StatusConflict)
	api.request("unsupported message type rejected", http.MethodPost, "/api/v1/chat/channels/"+channelID+"/messages", actorToken, map[string]any{"client_message_id": "invalid-type", "type": "video", "content": "x"}, http.StatusBadRequest)
	api.request("empty message content rejected", http.MethodPost, "/api/v1/chat/channels/"+channelID+"/messages", actorToken, map[string]any{"client_message_id": "empty-content", "type": "text", "content": "  "}, http.StatusBadRequest)

	secondaryBody := api.request("create second direct channel", http.MethodPost, "/api/v1/chat/channels/direct", actorToken, map[string]any{"peer_id": "smoke-photographer-04"}, http.StatusOK)
	secondaryID := stringField(dataMap(secondaryBody), "id")
	secondaryMsg := api.request("create message in second channel", http.MethodPost, "/api/v1/chat/channels/"+secondaryID+"/messages", actorToken, map[string]any{"client_message_id": "secondary-message", "type": "text", "content": "another conversation"}, http.StatusCreated)
	foreignMessageID := stringField(dataMap(secondaryMsg), "id")
	image := api.request("create image reference message", http.MethodPost, "/api/v1/chat/channels/"+secondaryID+"/messages", actorToken, map[string]any{"client_message_id": "image-smoke-001", "type": "image", "content": "https://assets.invalid/sample.jpg"}, http.StatusCreated)
	api.check("image type and reference are preserved", stringField(dataMap(image), "type") == "image" && stringField(dataMap(image), "content") == "https://assets.invalid/sample.jpg")
	file := api.request("create file reference message", http.MethodPost, "/api/v1/chat/channels/"+secondaryID+"/messages", actorToken, map[string]any{"client_message_id": "file-smoke-001", "type": "file", "content": "object-storage://bucket/document.pdf"}, http.StatusCreated)
	api.check("file type and reference are preserved", stringField(dataMap(file), "type") == "file" && stringField(dataMap(file), "content") == "object-storage://bucket/document.pdf")
	api.request("cross-channel reply rejected", http.MethodPost, "/api/v1/chat/channels/"+channelID+"/messages", actorToken, map[string]any{"client_message_id": "bad-cross-reply", "type": "text", "content": "reply", "reply_to_message_id": foreignMessageID}, http.StatusBadRequest)

	reply := api.request("reply to message in same channel", http.MethodPost, "/api/v1/chat/channels/"+channelID+"/messages", peerToken, map[string]any{"client_message_id": "client-smoke-002", "type": "text", "content": "reply from peer", "reply_to_message_id": messageID}, http.StatusCreated)
	replyData := dataMap(reply)
	api.check("reply receives sequence 2", numberField(replyData, "seq") == 2)
	api.check("reply links to original message", stringField(replyData, "reply_to_message_id") == messageID)

	history := api.request("list channel messages", http.MethodGet, "/api/v1/chat/channels/"+channelID+"/messages?limit=20&before_seq=0", peerToken, nil, http.StatusOK)
	historyData, historyOK := history["data"].([]any)
	api.check("history returns both messages newest first", historyOK && len(historyData) == 2 && numberField(asMap(historyData[0]), "seq") == 2 && numberField(asMap(historyData[1]), "seq") == 1)
	page := api.request("history before_seq pagination", http.MethodGet, "/api/v1/chat/channels/"+channelID+"/messages?limit=1&before_seq=2", peerToken, nil, http.StatusOK)
	pageData, pageOK := page["data"].([]any)
	api.check("before_seq excludes the boundary sequence", pageOK && len(pageData) == 1 && numberField(asMap(pageData[0]), "seq") == 1)
	api.request("message limit above 100 rejected", http.MethodGet, "/api/v1/chat/channels/"+channelID+"/messages?limit=101", peerToken, nil, http.StatusBadRequest)
	api.request("zero message limit rejected", http.MethodGet, "/api/v1/chat/channels/"+channelID+"/messages?limit=0", peerToken, nil, http.StatusBadRequest)
	api.request("negative before_seq rejected", http.MethodGet, "/api/v1/chat/channels/"+channelID+"/messages?before_seq=-1", peerToken, nil, http.StatusBadRequest)
	api.request("history hidden from outsider", http.MethodGet, "/api/v1/chat/channels/"+channelID+"/messages", outsiderToken, nil, http.StatusNotFound)

	edited := api.request("sender edits own message", http.MethodPatch, "/api/v1/chat/messages/"+messageID, actorToken, map[string]any{"content": "edited by sender"}, http.StatusOK)
	api.check("edit increments revision and updates content", numberField(dataMap(edited), "revision") == 2 && stringField(dataMap(edited), "content") == "edited by sender")
	api.request("other participant cannot edit sender message", http.MethodPatch, "/api/v1/chat/messages/"+messageID, peerToken, map[string]any{"content": "not allowed"}, http.StatusForbidden)
	api.request("empty edit content rejected", http.MethodPatch, "/api/v1/chat/messages/"+messageID, actorToken, map[string]any{"content": " "}, http.StatusBadRequest)

	recalled := api.request("sender recalls own message", http.MethodPost, "/api/v1/chat/messages/"+messageID+"/recall", actorToken, nil, http.StatusOK)
	api.check("recall clears message content", stringField(dataMap(recalled), "content") == "")
	api.request("other participant cannot recall sender message", http.MethodPost, "/api/v1/chat/messages/"+stringField(replyData, "id")+"/recall", actorToken, nil, http.StatusForbidden)
	api.request("repeated recall is idempotent", http.MethodPost, "/api/v1/chat/messages/"+messageID+"/recall", actorToken, nil, http.StatusOK)
	api.request("recalled message cannot be edited", http.MethodPatch, "/api/v1/chat/messages/"+messageID, actorToken, map[string]any{"content": "restore"}, http.StatusConflict)
	api.request("malformed message id rejected", http.MethodPatch, "/api/v1/chat/messages/not-an-object-id", actorToken, map[string]any{"content": "x"}, http.StatusBadRequest)

	read := api.request("advance read cursor", http.MethodPut, "/api/v1/chat/channels/"+channelID+"/read-cursor", peerToken, map[string]any{"last_read_seq": 2}, http.StatusOK)
	api.check("read cursor advances to sequence 2", numberField(dataMap(read), "last_read_seq") == 2)
	regressed := api.request("read cursor never regresses", http.MethodPut, "/api/v1/chat/channels/"+channelID+"/read-cursor", peerToken, map[string]any{"last_read_seq": 1}, http.StatusOK)
	api.check("read cursor remains at sequence 2", numberField(dataMap(regressed), "last_read_seq") == 2)
	api.request("read cursor past latest message rejected", http.MethodPut, "/api/v1/chat/channels/"+channelID+"/read-cursor", peerToken, map[string]any{"last_read_seq": 3}, http.StatusBadRequest)
	api.request("read cursor zero rejected", http.MethodPut, "/api/v1/chat/channels/"+channelID+"/read-cursor", peerToken, map[string]any{"last_read_seq": 0}, http.StatusBadRequest)
	api.request("outsider cannot advance read cursor", http.MethodPut, "/api/v1/chat/channels/"+channelID+"/read-cursor", outsiderToken, map[string]any{"last_read_seq": 1}, http.StatusNotFound)

	stopServer(cmd, done)
	serverStopped = true
	api.check("server stopped cleanly", true)
	dropTestDatabase(database)

	fmt.Printf("\nAPI smoke result: %d passed, %d failed\n", passed.Load(), failed.Load())
	if failed.Load() > 0 {
		os.Exit(1)
	}
}

type smokeAPI struct {
	client          *http.Client
	base            string
	lastBody        []byte
	lastContentType string
}

func (api *smokeAPI) request(name, method, route, token string, payload any, expectedStatus int) map[string]any {
	var body io.Reader
	if raw, ok := payload.(string); ok {
		body = strings.NewReader(raw)
	} else if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			api.check(name, false)
			return nil
		}
		body = strings.NewReader(string(encoded))
	}
	req, err := http.NewRequest(method, api.base+route, body)
	if err != nil {
		api.check(name, false)
		return nil
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := api.client.Do(req)
	if err != nil {
		fmt.Printf("FAIL %s: request error: %v\n", name, err)
		failed.Add(1)
		return nil
	}
	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		api.check(name, false)
		return nil
	}
	api.lastBody = bytes
	api.lastContentType = resp.Header.Get("Content-Type")
	api.check(fmt.Sprintf("%s (HTTP %d)", name, resp.StatusCode), resp.StatusCode == expectedStatus)
	var decoded map[string]any
	if len(bytes) > 0 && strings.Contains(api.lastContentType, "application/json") {
		if err := json.Unmarshal(bytes, &decoded); err != nil {
			fmt.Printf("FAIL %s response JSON: %v; body=%q\n", name, err, bytes)
			failed.Add(1)
			return nil
		}
	}
	return decoded
}

func (api *smokeAPI) check(name string, ok bool) {
	if ok {
		passed.Add(1)
		fmt.Println("PASS", name)
		return
	}
	failed.Add(1)
	fmt.Println("FAIL", name)
}

func dataMap(envelope map[string]any) map[string]any {
	if envelope == nil {
		return nil
	}
	data, _ := envelope["data"].(map[string]any)
	return data
}

func asMap(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func stringField(object map[string]any, key string) string {
	if object == nil {
		return ""
	}
	value, _ := object[key].(string)
	return value
}

func numberField(object map[string]any, key string) float64 {
	if object == nil {
		return -1
	}
	value, _ := object[key].(float64)
	return value
}

func makeToken(key *rsa.PrivateKey, subject string, expired bool) string {
	now := time.Now()
	expires := now.Add(time.Hour)
	if expired {
		expires = now.Add(-time.Hour)
	}
	claims := jwt.MapClaims{
		"iss": "https://smoke-test.invalid/realms/chat",
		"aud": []string{"chat-service"},
		"sub": subject,
		"typ": "Bearer",
		"iat": now.Unix(),
		"exp": expires.Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	signed, err := token.SignedString(key)
	if err != nil {
		fatal("sign temporary test token", err)
	}
	return signed
}

func buildServerBinary() (string, error) {
	file, err := os.CreateTemp("", "chat-service-local-api-*")
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		return "", err
	}
	_ = os.Remove(path)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-o", path, "./cmd/server")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return filepath.Clean(path), nil
}

func availablePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func waitForServer(client *http.Client, healthURL string, done chan error, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			fmt.Printf("chat-service exited before readiness: %v\n", err)
			done <- err
			return false
		default:
		}
		resp, err := client.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				passed.Add(1)
				fmt.Println("PASS service startup")
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func stopServer(cmd *exec.Cmd, done <-chan error) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case err := <-done:
		if err != nil {
			fmt.Printf("chat-service shutdown returned: %v\n", err)
		}
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

func dropTestDatabase(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err == nil {
		defer client.Disconnect(context.Background())
		err = client.Database(name).Drop(ctx)
	}
	if err != nil {
		fmt.Printf("FAIL drop ephemeral test database: %v\n", err)
		failed.Add(1)
		return
	}
	passed.Add(1)
	fmt.Println("PASS drop ephemeral test database")
}

func withEnv(overrides map[string]string) []string {
	env := os.Environ()
	filtered := make([]string, 0, len(env)+len(overrides))
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if _, replaced := overrides[key]; !replaced {
			filtered = append(filtered, entry)
		}
	}
	for key, value := range overrides {
		filtered = append(filtered, key+"="+value)
	}
	return filtered
}

func fatal(message string, err error) {
	fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", message, err)
	os.Exit(1)
}
