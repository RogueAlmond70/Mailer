package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "embed"

	"github.com/Mailer/internal"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed sql/001_schema.sql
var schemaSQL string

var _ internal.Database = (*PostgresDB)(nil)

type PostgresDB struct {
	DB *sql.DB
}

type emailRequest struct {
	Service    string     `json:"service"`
	ID         string     `json:"id"`
	From       string     `json:"from"`
	To         string     `json:"to"`
	Subject    string     `json:"subject"`
	Body       string     `json:"data"`
	Attachment attachment `json:"attachment"`
}

type attachment struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Content     []byte `json:"content"`
	ContentID   string `json:"content_id,omitempty"`
	Inline      bool   `json:"inline,omitempty"`
}

type deadLetter struct {
	OriginalMessage emailRequest `json:"original_message"`
	FailureReason   string       `json:"failure_reason"`
	Attempts        int          `json:"attempts"`
	FailedAt        time.Time    `json:"failed_at"`
}

func ConnectToDB(ctx context.Context, logger internal.Logger, cfg internal.Config) (*PostgresDB, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PostgresHost,
		cfg.PostgresPort,
		cfg.PostgresUser,
		cfg.PostgresPassword,
		cfg.PostgresDbName,
		cfg.PostgresSSLMode,
	)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		db.Close()
		logger.Error("failed to check database connection", "error", err)
		return nil, fmt.Errorf("failed to check database connection: %w", err)
	}

	pg := &PostgresDB{DB: db}
	if err = pg.CreateTableIfNotExists(ctx); err != nil {
		db.Close()
		logger.Error("failed to create table", "error", err)
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	return pg, nil
}

func (p *PostgresDB) Close() error {
	if p == nil || p.DB == nil {
		return nil
	}
	return p.DB.Close()
}

func (p *PostgresDB) CreateTableIfNotExists(ctx context.Context) error {
	for _, stmt := range splitSQL(schemaSQL) {
		if _, err := p.DB.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to apply schema: %w", err)
		}
	}
	return nil
}

func (p *PostgresDB) IsDuplicate(ctx context.Context, requestID string) (bool, error) {
	if requestID == "" {
		return false, errors.New("email id is required")
	}

	var exists bool
	err := p.DB.QueryRowContext(
		ctx,
		`SELECT EXISTS (SELECT 1 FROM email_records WHERE id = $1)`,
		requestID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check for duplicate email id: %w", err)
	}
	return exists, nil
}

func (p *PostgresDB) StoreEmailRecord(ctx context.Context, data []byte) error {
	var req emailRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return fmt.Errorf("failed to unmarshal email record: %w", err)
	}
	if err := req.validate(); err != nil {
		return fmt.Errorf("failed to store email record: %w", err)
	}

	_, err := p.DB.ExecContext(ctx, `
		INSERT INTO email_records (
			id, service, from_address, to_address, subject, body,
			attachment_filename, attachment_content_type, attachment_content,
			attachment_content_id, attachment_inline
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		req.ID,
		req.Service,
		req.From,
		req.To,
		req.Subject,
		req.Body,
		textOrNil(req.Attachment.Filename),
		textOrNil(req.Attachment.ContentType),
		bytesOrNil(req.Attachment.Content),
		textOrNil(req.Attachment.ContentID),
		req.Attachment.Inline,
	)
	if isUniqueViolation(err) {
		return fmt.Errorf("%w: %s", internal.ErrAlreadyStored, req.ID)
	}
	if err != nil {
		return fmt.Errorf("failed to store email record: %w", err)
	}
	return nil
}

func (p *PostgresDB) DeleteEmailRecord(ctx context.Context, requestID string) error {
	if requestID == "" {
		return errors.New("email id is required")
	}

	_, err := p.DB.ExecContext(ctx, `DELETE FROM email_records WHERE id = $1`, requestID)
	if err != nil {
		return fmt.Errorf("failed to delete email record: %w", err)
	}
	return nil
}

func (p *PostgresDB) UploadDeadLetter(ctx context.Context, data []byte) error {
	var letter deadLetter
	if err := json.Unmarshal(data, &letter); err != nil {
		return fmt.Errorf("failed to unmarshal dead letter: %w", err)
	}
	if err := letter.validate(); err != nil {
		return fmt.Errorf("failed to store dead letter: %w", err)
	}

	req := letter.OriginalMessage
	_, err := p.DB.ExecContext(ctx, `
		INSERT INTO dead_letters (
			request_id, service, from_address, to_address, subject, body,
			attachment_filename, attachment_content_type, attachment_content,
			attachment_content_id, attachment_inline,
			failure_reason, attempts, failed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		req.ID,
		req.Service,
		req.From,
		req.To,
		req.Subject,
		req.Body,
		textOrNil(req.Attachment.Filename),
		textOrNil(req.Attachment.ContentType),
		bytesOrNil(req.Attachment.Content),
		textOrNil(req.Attachment.ContentID),
		req.Attachment.Inline,
		letter.FailureReason,
		letter.Attempts,
		letter.FailedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to store dead letter: %w", err)
	}
	return nil
}

func (r emailRequest) validate() error {
	if r.ID == "" {
		return errors.New("email id is required")
	}
	if r.Service == "" {
		return errors.New("email service is required")
	}
	if r.From == "" {
		return errors.New("email sender is required")
	}
	if r.To == "" {
		return errors.New("email recipient is required")
	}
	if r.Body == "" {
		return errors.New("email body is required")
	}
	return nil
}

func (d deadLetter) validate() error {
	if err := d.OriginalMessage.validate(); err != nil {
		return err
	}
	if d.FailureReason == "" {
		return errors.New("failure reason is required")
	}
	if d.Attempts <= 0 {
		return errors.New("attempts must be greater than zero")
	}
	if d.FailedAt.IsZero() {
		return errors.New("failed at is required")
	}
	return nil
}

func textOrNil(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func bytesOrNil(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func splitSQL(script string) []string {
	parts := strings.Split(script, ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		stmt := strings.TrimSpace(part)
		if stmt == "" {
			continue
		}
		statements = append(statements, stmt)
	}
	return statements
}
