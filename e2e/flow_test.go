package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pubSub "github.com/Mailer/PubSub"
	"github.com/Mailer/internal"
	"github.com/Mailer/metrics"
	"github.com/Mailer/processing"
	"github.com/Mailer/worker"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
)

func TestEndToEndSuccessAndDuplicate(t *testing.T) {
	js, stream := startStack(t)
	sender := &scriptSender{}
	db := newMemDB()
	outcomes := watchOutcomes(t, stream)
	startWorker(t, js, stream, sender, db)

	publish(t, js, sampleRequest("sent-1"))
	first := waitOutcome(t, outcomes)
	if first.Status != processing.StatusSent || first.ID != "sent-1" {
		t.Fatalf("outcome = %+v", first)
	}
	if sender.Calls("sent-1") != 1 {
		t.Fatalf("sends = %d", sender.Calls("sent-1"))
	}

	publish(t, js, sampleRequest("sent-1"))
	second := waitOutcome(t, outcomes)
	if second.Status != processing.StatusSent || second.ID != "sent-1" {
		t.Fatalf("duplicate outcome = %+v", second)
	}
	if sender.Calls("sent-1") != 1 {
		t.Fatalf("duplicate sent again, sends = %d", sender.Calls("sent-1"))
	}
}

func TestEndToEndFailureIsAcked(t *testing.T) {
	js, stream := startStack(t)
	sender := &scriptSender{err: errors.New("dial tcp: connection refused")}
	db := newMemDB()
	outcomes := watchOutcomes(t, stream)
	startWorker(t, js, stream, sender, db)

	publish(t, js, sampleRequest("fail-1"))
	outcome := waitOutcome(t, outcomes)
	if outcome.Status != processing.StatusFailed || outcome.ID != "fail-1" || outcome.Error == "" {
		t.Fatalf("outcome = %+v", outcome)
	}
	if sender.Calls("fail-1") != 3 {
		t.Fatalf("sends = %d", sender.Calls("fail-1"))
	}
	if db.DeadLetters() != 1 {
		t.Fatalf("dead letters = %d", db.DeadLetters())
	}

	time.Sleep(300 * time.Millisecond)
	if sender.Calls("fail-1") != 3 {
		t.Fatalf("failed message was redelivered, sends = %d", sender.Calls("fail-1"))
	}
}

func TestRedeliveryWhenHandlerDoesNotAck(t *testing.T) {
	js, stream := startStack(t)
	ctx := context.Background()
	consumer, err := pubSub.EnsureConsumer(ctx, stream, internal.Config{
		AckWait:     time.Second,
		MaxDeliver:  5,
		MaxInFlight: 4,
	})
	if err != nil {
		t.Fatal(err)
	}

	var deliveries atomic.Int32
	done := make(chan struct{})
	var once sync.Once
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		if deliveries.Add(1) == 1 {
			return
		}
		if err := msg.Ack(); err != nil {
			t.Errorf("ack: %v", err)
		}
		once.Do(func() { close(done) })
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cc.Stop)

	if _, err := js.Publish(ctx, internal.SubjectSend, []byte("hello")); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatalf("redelivery timed out after %d deliveries", deliveries.Load())
	}
	if deliveries.Load() < 2 {
		t.Fatalf("deliveries = %d", deliveries.Load())
	}
}

func TestConsumerGroupDoesNotDoubleDeliver(t *testing.T) {
	js, stream := startStack(t)
	ctx := context.Background()
	consumer, err := pubSub.EnsureConsumer(ctx, stream, internal.Config{
		AckWait:     5 * time.Second,
		MaxDeliver:  3,
		MaxInFlight: 8,
	})
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	got := map[string]int{}
	handler := func(msg jetstream.Msg) {
		mu.Lock()
		got[string(msg.Data())]++
		mu.Unlock()
		if err := msg.Ack(); err != nil {
			t.Errorf("ack: %v", err)
		}
	}
	first, err := consumer.Consume(handler)
	if err != nil {
		t.Fatal(err)
	}
	second, err := consumer.Consume(handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(first.Stop)
	t.Cleanup(second.Stop)

	for _, id := range []string{"a", "b", "c", "d"} {
		if _, err := js.Publish(ctx, internal.SubjectSend, []byte(id)); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		complete := len(got) == 4
		mu.Unlock()
		if complete {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("delivered = %#v", got)
	}
	for id, count := range got {
		if count != 1 {
			t.Fatalf("message %s delivered %d times", id, count)
		}
	}
}

func startStack(t *testing.T) (jetstream.JetStream, jetstream.Stream) {
	t.Helper()
	url := startNATS(t)
	ctx := context.Background()

	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		nc, js, stream, err := pubSub.Setup(ctx, url)
		if err == nil {
			t.Cleanup(nc.Close)
			return js, stream
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("setup: %v", lastErr)
	return nil, nil
}

func startNATS(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      port,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(ns.Shutdown)
	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats is not ready")
	}
	return ns.ClientURL()
}

func startWorker(t *testing.T, js jetstream.JetStream, stream jetstream.Stream, sender *scriptSender, db *memDB) {
	t.Helper()
	cfg := internal.Config{
		MaxSendAttempts:   3,
		RetryBackoff:      time.Millisecond,
		MaxInFlight:       4,
		MaxDeliver:        5,
		AckWait:           30 * time.Second,
		ProcessingTimeout: 5 * time.Second,
		RedeliveryDelay:   50 * time.Millisecond,
	}
	consumer, err := pubSub.EnsureConsumer(context.Background(), stream, cfg)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mailer := processing.CreateMailer(logger, cfg, db, pubSub.JetStreamPublisher{JS: js}, sender, metrics.New())

	runCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.Run(runCtx, logger, consumer, nil, worker.Options{
			MaxInFlight:       cfg.MaxInFlight,
			ProcessingTimeout: cfg.ProcessingTimeout,
			RedeliveryDelay:   cfg.RedeliveryDelay,
		}, mailer.HandleMessage)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("worker: %v", err)
			}
		case <-time.After(6 * time.Second):
			t.Error("worker did not stop")
		}
	})
}

func watchOutcomes(t *testing.T, stream jetstream.Stream) <-chan processing.Outcome {
	t.Helper()
	consumer, err := stream.CreateOrUpdateConsumer(context.Background(), jetstream.ConsumerConfig{
		Durable:        "test-outcomes",
		AckPolicy:      jetstream.AckExplicitPolicy,
		FilterSubjects: []string{internal.SubjectSent, internal.SubjectFailed},
	})
	if err != nil {
		t.Fatal(err)
	}
	outcomes := make(chan processing.Outcome, 8)
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		var outcome processing.Outcome
		if err := json.Unmarshal(msg.Data(), &outcome); err != nil {
			t.Errorf("outcome: %v", err)
			_ = msg.Nak()
			return
		}
		outcomes <- outcome
		_ = msg.Ack()
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cc.Stop)
	return outcomes
}

func publish(t *testing.T, js jetstream.JetStream, request processing.EmailRequest) {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(context.Background(), internal.SubjectSend, body); err != nil {
		t.Fatal(err)
	}
}

func waitOutcome(t *testing.T, outcomes <-chan processing.Outcome) processing.Outcome {
	t.Helper()
	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for outcome")
	}
	return processing.Outcome{}
}

func sampleRequest(id string) processing.EmailRequest {
	return processing.EmailRequest{
		Service: "band-builder",
		ID:      id,
		From:    "mailer@example.com",
		To:      "musician@example.com",
		Subject: "Hello",
		Body:    "<p>Hello</p>",
	}
}

type scriptSender struct {
	mu    sync.Mutex
	err   error
	calls map[string]int
}

func (s *scriptSender) Send(_ context.Context, request processing.EmailRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls == nil {
		s.calls = map[string]int{}
	}
	s.calls[request.ID]++
	return s.err
}

func (s *scriptSender) Calls(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[id]
}

type memDB struct {
	mu      sync.Mutex
	records map[string][]byte
	dead    int
}

func newMemDB() *memDB {
	return &memDB{records: map[string][]byte{}}
}

func (m *memDB) IsDuplicate(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.records[id]
	return ok, nil
}

func (m *memDB) StoreEmailRecord(_ context.Context, data []byte) error {
	var request processing.EmailRequest
	if err := json.Unmarshal(data, &request); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.records[request.ID]; ok {
		return internal.ErrAlreadyStored
	}
	m.records[request.ID] = data
	return nil
}

func (m *memDB) DeleteEmailRecord(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, id)
	return nil
}

func (m *memDB) UploadDeadLetter(context.Context, []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dead++
	return nil
}

func (m *memDB) CreateTableIfNotExists(context.Context) error { return nil }

func (m *memDB) DeadLetters() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dead
}
