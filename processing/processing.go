package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"time"

	"github.com/Mailer/internal"
	"github.com/Mailer/metrics"
)

var (
	ErrNoId                   = errors.New("ID not provided in email")
	ErrNoRecipient            = errors.New("no email recipient")
	ErrNoSender               = errors.New("no sender")
	ErrNoTemplate             = errors.New("no email template")
	ErrNoBody                 = errors.New("no email body")
	ErrNoService              = errors.New("no requesting service specified")
	ErrDuplicateId            = errors.New("duplicate email id")
	ErrMaxSendAttemptsReached = errors.New("maximum send attempts reached")
	// ErrRedeliver asks the caller to Nak the JetStream message.
	// A nil return is terminal: the message has been sent, skipped, or dead-lettered, and should be acked.
	ErrRedeliver = errors.New("redeliver")
)

const (
	StatusSent   = "sent"
	StatusFailed = "failed"
)

// Publisher emits email.sent and email.failed.
type Publisher interface {
	Publish(ctx context.Context, subject string, payload []byte) error
}

// Outcome is the body published on email.sent and email.failed.
type Outcome struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Status  string `json:"status"`
	Error   string `json:"error,omitempty"`
}

type Mailer struct {
	logger    internal.Logger
	cfg       internal.Config
	db        internal.Database
	publisher Publisher
	sender    Sender
	metrics   *metrics.Metrics
	sleepFn   func(context.Context, time.Duration) error
}

func CreateMailer(logger internal.Logger, cfg internal.Config, db internal.Database, publisher Publisher, sender Sender, m *metrics.Metrics) *Mailer {
	if m == nil {
		m = metrics.New()
	}
	if cfg.MaxSendAttempts < 1 {
		cfg.MaxSendAttempts = 1
	}
	return &Mailer{
		logger:    logger,
		cfg:       cfg,
		db:        db,
		publisher: publisher,
		sender:    sender,
		metrics:   m,
		sleepFn:   sleep,
	}
}

// HandleMessage processes one email.send payload.
// A nil error means the JetStream message should be acknowledged.
// ErrRedeliver means it should be negatively acknowledged.
func (m *Mailer) HandleMessage(ctx context.Context, data []byte) error {
	start := time.Now()
	defer func() {
		m.metrics.Processing.Observe(time.Since(start).Seconds())
	}()

	if err := ctx.Err(); err != nil {
		return redeliver(err)
	}

	var request EmailRequest
	if err := json.Unmarshal(data, &request); err != nil {
		m.metrics.Failed.Inc()
		m.logger.Error("dropping invalid payload", "error", err)
		return nil
	}

	if err := m.validateEmail(request); err != nil {
		m.logger.Error("failed to validate email", "request ID", request.ID, "error", err)
		return m.reject(ctx, request, 1, err)
	}

	duplicate, err := m.db.IsDuplicate(ctx, request.ID)
	if err != nil {
		m.logger.Error("failed to check for duplicate email id", "request ID", request.ID, "error", err)
		return redeliver(err)
	}
	if duplicate {
		m.metrics.Duplicate.Inc()
		m.logger.Info("email already sent", "request ID", request.ID)
		return m.publish(ctx, internal.SubjectSent, request, "")
	}

	attempts, err := m.sendWithRetry(ctx, request)
	if err != nil {
		if errors.Is(err, ErrRedeliver) {
			return err
		}
		m.logger.Error("failed to send email", "request ID", request.ID, "attempts", attempts, "error", err)
		return m.reject(ctx, request, attempts, err)
	}

	stored, err := json.Marshal(request)
	if err != nil {
		return redeliver(err)
	}
	if err := m.db.StoreEmailRecord(ctx, stored); err != nil {
		if errors.Is(err, internal.ErrAlreadyStored) {
			m.metrics.Duplicate.Inc()
			m.logger.Info("email already recorded", "request ID", request.ID)
			return m.publish(ctx, internal.SubjectSent, request, "")
		}
		m.logger.Error("failed to save email record", "request ID", request.ID, "error", err)
		return redeliver(err)
	}

	m.metrics.Sent.Inc()
	m.logger.Info("sent email", "request ID", request.ID)
	return m.publish(ctx, internal.SubjectSent, request, "")
}

func (m *Mailer) validateEmail(request EmailRequest) error {
	if request.Service == "" {
		return ErrNoService
	}
	if request.ID == "" {
		return ErrNoId
	}
	if request.From == "" {
		return ErrNoSender
	}
	if request.To == "" {
		return ErrNoRecipient
	}
	if request.Body == "" {
		return ErrNoBody
	}
	if _, err := mail.ParseAddress(request.From); err != nil {
		return fmt.Errorf("failed to validate 'From' email: %w", err)
	}
	if _, err := mail.ParseAddress(request.To); err != nil {
		return fmt.Errorf("failed to validate 'To' email: %w", err)
	}
	return nil
}

func (m *Mailer) sendWithRetry(ctx context.Context, request EmailRequest) (int, error) {
	var last error
	attempts := 0
	for attempts < m.cfg.MaxSendAttempts {
		if err := ctx.Err(); err != nil {
			return attempts, redeliver(err)
		}
		attempts++
		m.metrics.Attempts.Inc()
		err := m.sender.Send(ctx, request)
		if err == nil {
			return attempts, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return attempts, redeliver(err)
		}
		last = err
		m.logger.Warn("email send attempt failed", "request ID", request.ID, "attempt", attempts, "error", err)
		if IsPermanent(err) || attempts >= m.cfg.MaxSendAttempts {
			break
		}
		if err := m.sleepFn(ctx, m.backoff(attempts)); err != nil {
			return attempts, redeliver(err)
		}
	}
	if IsPermanent(last) {
		return attempts, last
	}
	return attempts, fmt.Errorf("%w: %v", ErrMaxSendAttemptsReached, last)
}

func (m *Mailer) backoff(attempt int) time.Duration {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 5 {
		shift = 5
	}
	return m.cfg.RetryBackoff * time.Duration(1<<shift)
}

func (m *Mailer) reject(ctx context.Context, request EmailRequest, attempts int, reason error) error {
	if !canDeadLetter(request) {
		m.metrics.Failed.Inc()
		m.logger.Error("dropping invalid message", "request ID", request.ID, "error", reason)
		return nil
	}

	letter := DeadLetter{
		OriginalMessage: request,
		FailureReason:   reason.Error(),
		Attempts:        attempts,
		FailedAt:        time.Now(),
	}
	if letter.Attempts < 1 {
		letter.Attempts = 1
	}
	if err := m.addToDeadLetterQueue(ctx, letter); err != nil {
		m.logger.Error("failed to add to dead letter queue", "request ID", request.ID, "error", err)
		return redeliver(err)
	}
	if err := m.publish(ctx, internal.SubjectFailed, request, reason.Error()); err != nil {
		return err
	}
	m.metrics.Failed.Inc()
	return nil
}

func (m *Mailer) publish(ctx context.Context, subject string, request EmailRequest, failure string) error {
	status := StatusSent
	if subject == internal.SubjectFailed {
		status = StatusFailed
	}
	body, err := json.Marshal(Outcome{
		ID:      request.ID,
		Service: request.Service,
		Status:  status,
		Error:   failure,
	})
	if err != nil {
		return redeliver(err)
	}
	if err := m.publisher.Publish(ctx, subject, body); err != nil {
		m.logger.Error("failed to publish outcome", "request ID", request.ID, "subject", subject, "error", err)
		return redeliver(err)
	}
	m.logger.Info("published outcome", "request ID", request.ID, "subject", subject)
	return nil
}

func (m *Mailer) addToDeadLetterQueue(ctx context.Context, deadLetter DeadLetter) error {
	dl, err := json.Marshal(deadLetter)
	if err != nil {
		return fmt.Errorf("failed to add to dead letter queue: %w", err)
	}
	if err := m.db.UploadDeadLetter(ctx, dl); err != nil {
		return fmt.Errorf("failed to add to dead letter queue: %w", err)
	}
	m.logger.Info("stored dead letter", "request ID", deadLetter.OriginalMessage.ID, "attempts", deadLetter.Attempts)
	return nil
}

func canDeadLetter(request EmailRequest) bool {
	return request.ID != "" && request.Service != "" && request.From != "" && request.To != "" && request.Body != ""
}

func redeliver(err error) error {
	if err == nil {
		return ErrRedeliver
	}
	return fmt.Errorf("%w: %w", ErrRedeliver, err)
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
