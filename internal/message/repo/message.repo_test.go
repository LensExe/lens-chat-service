package repo

import (
	"context"
	"errors"
	"fmt"
	channelrepo "go-app/internal/channel/repo"
	"go-app/internal/schema"
	"go-app/pkg/response"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMongoTransactionsAndIdempotency(t *testing.T) {
	uri := os.Getenv("CHAT_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("set CHAT_TEST_MONGO_URI to run replica set integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	db := client.Database("chat_test_" + primitive.NewObjectID().Hex())
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		db.Drop(cleanup)
		client.Disconnect(cleanup)
	}()
	cr := channelrepo.New(db)
	mr := New(db)
	if err = cr.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	if err = mr.EnsureIndexes(ctx); err != nil {
		t.Fatal(err)
	}
	ch, err := cr.GetOrCreate(ctx, "tenant", [2]string{"a", "b"}, "pair")
	if err != nil {
		t.Fatal(err)
	}
	ch2, err := cr.GetOrCreate(ctx, "tenant", [2]string{"a", "b"}, "pair")
	if err != nil || ch2.ID != ch.ID {
		t.Fatalf("duplicate channel: %v", err)
	}
	makeMessage := func(key, hash string) *schema.Message {
		return &schema.Message{TenantID: "tenant", ChannelID: ch.ID, SenderID: "a", ClientMessageID: key, RequestHash: hash, Type: "text", Content: "hello"}
	}
	var wg sync.WaitGroup
	ids := make(chan primitive.ObjectID, 12)
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, _, e := mr.Create(ctx, makeMessage("same-key", "hash"))
			if e != nil {
				errs <- e
				return
			}
			ids <- msg.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if t.Failed() {
		return
	}
	var first primitive.ObjectID
	for id := range ids {
		if first.IsZero() {
			first = id
		}
		if first != id {
			t.Fatal("duplicate retry inserted multiple messages")
		}
	}
	if _, _, err = mr.Create(ctx, makeMessage("same-key", "changed-hash")); !errors.Is(err, response.ErrConflict) {
		t.Fatal("idempotency conflict not rejected")
	}
	stored, err := cr.Get(ctx, "tenant", ch.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastMessageSeq != 1 {
		t.Fatalf("duplicate retries incremented sequence: %d", stored.LastMessageSeq)
	}
	errs = make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, e := mr.Create(ctx, makeMessage(fmt.Sprint(i), "hash"))
			if e != nil {
				errs <- e
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if t.Failed() {
		return
	}
	history, err := mr.List(ctx, "tenant", ch.ID, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 13 {
		t.Fatalf("count=%d", len(history))
	}
	for i, m := range history {
		if m.Sequence != int64(13-i) {
			t.Fatal("non-contiguous sequence")
		}
	}
	if seq, e := mr.Read(ctx, "tenant", ch.ID, "b", 10); e != nil || seq != 10 {
		t.Fatalf("read cursor: %d %v", seq, e)
	}
	if seq, e := mr.Read(ctx, "tenant", ch.ID, "b", 2); e != nil || seq != 10 {
		t.Fatalf("cursor regressed: %d %v", seq, e)
	}
	recalled, err := mr.Recall(ctx, &history[0])
	if err != nil || recalled.Content != "" {
		t.Fatalf("recall: %v", err)
	}
	if _, err = mr.Edit(ctx, &history[0], "resurrect"); !errors.Is(err, response.ErrConflict) {
		t.Fatal("recalled message edited")
	}
}
