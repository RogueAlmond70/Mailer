package processing

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Mailer/internal"
	"github.com/Mailer/metrics"
	dto "github.com/prometheus/client_model/go"
)

func TestHandleMessageSuccess(t *testing.T) {
	mailer, sender, db, publisher := testMailer(t)
	err := mailer.HandleMessage(context.Background(), mustJSON(t, sampleRequest("sent-1")))
	if err != nil {
		t.Fatal(err)
	}
	if sender.calls != 1 {
		t.Fatalf("sends = %d", sender.calls)
	}
	if _, ok := db.records["sent-1"]; !ok {
		t.Fatal("record was not stored")
	}
	if len(publisher.subjects) != 1 || publisher.subjects[0] != internal.SubjectSent {
		t.Fatalf("subjects = %v", publisher.subjects)
	}
	if counterValue(t, mailer.metrics.Sent) != 1 {
		t.Fatalf("sent metric = %v", counterValue(t, mailer.metrics.Sent))
	}
}

func TestHandleMessageDuplicateSkipsProvider(t *testing.T) {
	mailer, sender, db, publisher := testMailer(t)
	db.records["dup-1"] = []byte(`{}`)

	err := mailer.HandleMessage(context.Background(), mustJSON(t, sampleRequest("dup-1")))
	if err != nil {
		t.Fatal(err)
	}
	if sender.calls != 0 {
		t.Fatalf("sends = %d", sender.calls)
	}
	if publisher.subjects[0] != internal.SubjectSent {
		t.Fatalf("subjects = %v", publisher.subjects)
	}
	if counterValue(t, mailer.metrics.Duplicate) != 1 {
		t.Fatal("duplicate was not counted")
	}
}

func TestHandleMessageRetriesThenSends(t *testing.T) {
	mailer, sender, _, publisher := testMailer(t)
	sender.errs = []error{errors.New("421 try later"), errors.New("dial tcp: connection refused")}

	err := mailer.HandleMessage(context.Background(), mustJSON(t, sampleRequest("retry-1")))
	if err != nil {
		t.Fatal(err)
	}
	if sender.calls != 3 {
		t.Fatalf("sends = %d", sender.calls)
	}
	if publisher.subjects[0] != internal.SubjectSent {
		t.Fatalf("subjects = %v", publisher.subjects)
	}
}

func TestHandleMessageExhaustedRetriesDeadLetters(t *testing.T) {
	mailer, sender, db, publisher := testMailer(t)
	sender.err = errors.New("dial tcp: connection refused")

	err := mailer.HandleMessage(context.Background(), mustJSON(t, sampleRequest("fail-1")))
	if err != nil {
		t.Fatal(err)
	}
	if sender.calls != 3 {
		t.Fatalf("sends = %d", sender.calls)
	}
	if len(db.dead) != 1 {
		t.Fatalf("dead letters = %d", len(db.dead))
	}
	if publisher.subjects[0] != internal.SubjectFailed {
		t.Fatalf("subjects = %v", publisher.subjects)
	}
	var outcome Outcome
	if err := json.Unmarshal(publisher.payloads[0], &outcome); err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusFailed || outcome.ID != "fail-1" || outcome.Error == "" {
		t.Fatalf("outcome = %+v", outcome)
	}
	if _, stored := db.records["fail-1"]; stored {
		t.Fatal("failed send was recorded as sent")
	}
}

func TestHandleMessagePermanentProviderFailureDoesNotRetry(t *testing.T) {
	mailer, sender, db, publisher := testMailer(t)
	sender.err = Permanent(errors.New("550 no such user"))

	err := mailer.HandleMessage(context.Background(), mustJSON(t, sampleRequest("perm-1")))
	if err != nil {
		t.Fatal(err)
	}
	if sender.calls != 1 {
		t.Fatalf("sends = %d", sender.calls)
	}
	if len(db.dead) != 1 || publisher.subjects[0] != internal.SubjectFailed {
		t.Fatalf("dead=%d subjects=%v", len(db.dead), publisher.subjects)
	}
}

func TestHandleMessageInvalidAddressIsTerminal(t *testing.T) {
	mailer, sender, db, publisher := testMailer(t)
	request := sampleRequest("bad-addr")
	request.To = "not-an-email"

	err := mailer.HandleMessage(context.Background(), mustJSON(t, request))
	if err != nil {
		t.Fatal(err)
	}
	if sender.calls != 0 || len(db.dead) != 1 || publisher.subjects[0] != internal.SubjectFailed {
		t.Fatalf("sends=%d dead=%d subjects=%v", sender.calls, len(db.dead), publisher.subjects)
	}
}

func TestHandleMessageDropsPayloadThatCannotBeStored(t *testing.T) {
	mailer, sender, db, publisher := testMailer(t)
	err := mailer.HandleMessage(context.Background(), []byte(`{"id":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	if sender.calls != 0 || len(db.dead) != 0 || len(publisher.subjects) != 0 {
		t.Fatalf("sends=%d dead=%d subjects=%v", sender.calls, len(db.dead), publisher.subjects)
	}
	if counterValue(t, mailer.metrics.Failed) != 1 {
		t.Fatal("drop was not counted")
	}
}

func TestHandleMessageDuplicateCheckFailureRedelivers(t *testing.T) {
	mailer, sender, _, _ := testMailer(t)
	mailer.db.(*memDB).dupErr = errors.New("db down")

	err := mailer.HandleMessage(context.Background(), mustJSON(t, sampleRequest("db-1")))
	if !errors.Is(err, ErrRedeliver) {
		t.Fatalf("err = %v", err)
	}
	if sender.calls != 0 {
		t.Fatalf("sends = %d", sender.calls)
	}
}

func TestHandleMessageShutdownDuringBackoffRedelivers(t *testing.T) {
	mailer, sender, db, _ := testMailer(t)
	sender.err = errors.New("421 try later")
	mailer.sleepFn = func(context.Context, time.Duration) error {
		return context.Canceled
	}

	err := mailer.HandleMessage(context.Background(), mustJSON(t, sampleRequest("stop-1")))
	if !errors.Is(err, ErrRedeliver) {
		t.Fatalf("err = %v", err)
	}
	if sender.calls != 1 || len(db.dead) != 0 {
		t.Fatalf("sends=%d dead=%d", sender.calls, len(db.dead))
	}
}

func TestHandleMessagePublishFailureRedeliversAfterStore(t *testing.T) {
	mailer, _, db, publisher := testMailer(t)
	publisher.err = errors.New("nats down")

	err := mailer.HandleMessage(context.Background(), mustJSON(t, sampleRequest("pub-1")))
	if !errors.Is(err, ErrRedeliver) {
		t.Fatalf("err = %v", err)
	}
	if _, ok := db.records["pub-1"]; !ok {
		t.Fatal("record should be stored before publish")
	}
}

func TestHandleMessageAlreadyStoredPublishesSent(t *testing.T) {
	mailer, _, db, publisher := testMailer(t)
	db.storeErr = internal.ErrAlreadyStored

	err := mailer.HandleMessage(context.Background(), mustJSON(t, sampleRequest("race-1")))
	if err != nil {
		t.Fatal(err)
	}
	if publisher.subjects[0] != internal.SubjectSent {
		t.Fatalf("subjects = %v", publisher.subjects)
	}
}

func testMailer(t *testing.T) (*Mailer, *scriptSender, *memDB, *scriptPublisher) {
	t.Helper()
	sender := &scriptSender{}
	db := newMemDB()
	publisher := &scriptPublisher{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mailer := CreateMailer(logger, internal.Config{
		MaxSendAttempts: 3,
		RetryBackoff:    time.Hour,
	}, db, publisher, sender, metrics.New())
	mailer.sleepFn = func(context.Context, time.Duration) error { return nil }
	return mailer, sender, db, publisher
}

func sampleRequest(id string) EmailRequest {
	return EmailRequest{
		Service: "band-builder",
		ID:      id,
		From:    "mailer@example.com",
		To:      "musician@example.com",
		Subject: "Hello",
		Body:    "<p>Hello</p>",
	}
}

func mustJSON(t *testing.T, request EmailRequest) []byte {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func counterValue(t *testing.T, counter interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

type scriptSender struct {
	mu    sync.Mutex
	err   error
	errs  []error
	calls int
}

func (s *scriptSender) Send(context.Context, EmailRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if len(s.errs) > 0 {
		err := s.errs[0]
		s.errs = s.errs[1:]
		return err
	}
	return s.err
}

type scriptPublisher struct {
	mu       sync.Mutex
	err      error
	subjects []string
	payloads [][]byte
}

func (p *scriptPublisher) Publish(_ context.Context, subject string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subjects = append(p.subjects, subject)
	p.payloads = append(p.payloads, payload)
	return p.err
}

type memDB struct {
	mu       sync.Mutex
	records  map[string][]byte
	dead     [][]byte
	dupErr   error
	storeErr error
}

func newMemDB() *memDB {
	return &memDB{records: map[string][]byte{}}
}

func (m *memDB) IsDuplicate(_ context.Context, id string) (bool, error) {
	if m.dupErr != nil {
		return false, m.dupErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.records[id]
	return ok, nil
}

func (m *memDB) StoreEmailRecord(_ context.Context, data []byte) error {
	if m.storeErr != nil {
		return m.storeErr
	}
	var request EmailRequest
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

func (m *memDB) UploadDeadLetter(_ context.Context, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dead = append(m.dead, data)
	return nil
}

func (m *memDB) CreateTableIfNotExists(context.Context) error { return nil }
