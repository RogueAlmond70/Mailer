package storage

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Mailer/internal"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestSchemaIncludesBothTables(t *testing.T) {
	if !strings.Contains(schemaSQL, "CREATE TABLE IF NOT EXISTS email_records") {
		t.Fatal("schema missing email_records")
	}
	if !strings.Contains(schemaSQL, "CREATE TABLE IF NOT EXISTS dead_letters") {
		t.Fatal("schema missing dead_letters")
	}
	statements := splitSQL(schemaSQL)
	if len(statements) < 3 {
		t.Fatalf("statements = %d", len(statements))
	}
}

func TestUniqueViolation(t *testing.T) {
	if !isUniqueViolation(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("expected unique violation")
	}
	if isUniqueViolation(&pgconn.PgError{Code: "23502"}) {
		t.Fatal("not-null violation is not a duplicate")
	}
	if isUniqueViolation(errors.New("dial tcp")) {
		t.Fatal("network error is not a duplicate")
	}
}

func TestMethodsRejectBadPayloadsWithoutDatabase(t *testing.T) {
	ctx := context.Background()
	db := &PostgresDB{}

	if _, err := db.IsDuplicate(ctx, ""); err == nil {
		t.Fatal("expected empty id error")
	}
	if err := db.DeleteEmailRecord(ctx, ""); err == nil {
		t.Fatal("expected empty id error")
	}
	if err := db.StoreEmailRecord(ctx, []byte("{")); err == nil {
		t.Fatal("expected unmarshal error")
	}
	if err := db.StoreEmailRecord(ctx, []byte(`{"id":"1"}`)); err == nil {
		t.Fatal("expected validation error")
	}
	if err := db.UploadDeadLetter(ctx, []byte("{")); err == nil {
		t.Fatal("expected unmarshal error")
	}
	if err := db.UploadDeadLetter(ctx, []byte(`{"original_message":{"id":"1","service":"band-builder","from":"a@example.com","to":"b@example.com","data":"hi"},"failure_reason":"nope","attempts":0,"failed_at":"2026-01-15T12:00:00Z"}`)); err == nil {
		t.Fatal("expected attempts validation error")
	}
}

func TestPostgresRoundTrip(t *testing.T) {
	if os.Getenv("POSTGRES_TEST") == "" {
		t.Skip("set POSTGRES_TEST=1 to run against a local mailer database")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := internal.Config{
		PostgresHost:     envOr("POSTGRES_HOST", "localhost"),
		PostgresPort:     5432,
		PostgresUser:     envOr("POSTGRES_USER", "mailer"),
		PostgresPassword: envOr("POSTGRES_PASSWORD", "mailer"),
		PostgresDbName:   envOr("POSTGRES_DB_NAME", "mailer"),
		PostgresSSLMode:  "disable",
	}
	db, err := ConnectToDB(ctx, testLogger{}, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	id := "storage-test-" + time.Now().UTC().Format("20060102150405.000000000")
	payload := []byte(`{"service":"band-builder","id":"` + id + `","from":"mailer@example.com","to":"musician@example.com","subject":"Hi","data":"<p>Hi</p>"}`)

	duplicate, err := db.IsDuplicate(ctx, id)
	if err != nil || duplicate {
		t.Fatalf("duplicate before insert = %v, %v", duplicate, err)
	}
	if err := db.StoreEmailRecord(ctx, payload); err != nil {
		t.Fatalf("store: %v", err)
	}
	duplicate, err = db.IsDuplicate(ctx, id)
	if err != nil || !duplicate {
		t.Fatalf("duplicate after insert = %v, %v", duplicate, err)
	}
	if err := db.StoreEmailRecord(ctx, payload); !errors.Is(err, internal.ErrAlreadyStored) {
		t.Fatalf("second store = %v", err)
	}

	letter := []byte(`{"original_message":{"service":"band-builder","id":"` + id + `","from":"mailer@example.com","to":"musician@example.com","subject":"Hi","data":"<p>Hi</p>"},"failure_reason":"maximum send attempts reached","attempts":3,"failed_at":"2026-01-15T12:00:00Z"}`)
	if err := db.UploadDeadLetter(ctx, letter); err != nil {
		t.Fatalf("dead letter: %v", err)
	}
	if err := db.DeleteEmailRecord(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	duplicate, err = db.IsDuplicate(ctx, id)
	if err != nil || duplicate {
		t.Fatalf("duplicate after delete = %v, %v", duplicate, err)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

type testLogger struct{}

func (testLogger) Info(string, ...any)  {}
func (testLogger) Error(string, ...any) {}
func (testLogger) Warn(string, ...any)  {}
