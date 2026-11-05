package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/mail"
	"time"

	"github.com/Mailer/internal"
	"github.com/nats-io/nats.go"
	"gopkg.in/gomail.v2"
)

var (
	ErrNoId                   = errors.New("ID not provided in email")
	ErrNoRecipient            = errors.New("no email recipient")
	ErrNoSender               = errors.New("no sender")
	ErrNoTemplate             = errors.New("no email template")
	ErrNoBody                 = errors.New("no email body")
	ErrDuplicateId            = errors.New("duplicate email id")
	ErrMaxSendAttemptsReached = errors.New("maximum send attempts reached")
)

type Mailer struct {
	logger internal.Logger
	cfg    internal.Config
	db     internal.Database
}

type EmailRequest struct { // This type will need to be exported and used by all our other services
	ID         string     `json:"id"`
	From       string     `json:"from"`
	To         string     `json:"to"`
	Subject    string     `json:"subject"`
	Body       string     `json:"data"`
	Attachment Attachment `json:"attachment"`
}

type Attachment struct { // This type will need to be exported and used by all our other services
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Content     []byte `json:"content"`
	ContentID   string `json:"content_id,omitempty"` // for embedded content, this is the position within the email
	Inline      bool   `json:"inline,omitempty"`
}

type DeadLetter struct {
	OriginalMessage EmailRequest
	FailureReason   string
	Attempts        int
	FailedAt        time.Time
}

func (m *Mailer) handleMessage(ctx context.Context, msg *nats.Msg) error {
	if ctx.Err() != nil {
		m.logger.Error("failed to handle message", "error", ctx.Err())
		return fmt.Errorf("failed to handle message %w", ctx.Err())
	}

	var request EmailRequest

	err := json.Unmarshal(msg.Data, &request)
	if err != nil {
		m.logger.Error("failed to unmarshal payload", "error", err)
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	if request.ID == "" {
		m.logger.Error("failed to handle message", "error", ErrNoId)
		return fmt.Errorf("failed to handle message %w", ErrNoId)
	}
	if request.To == "" {
		m.logger.Error("failed to handle message", "error", ErrNoRecipient)
		return fmt.Errorf("failed to handle message: %w", ErrNoRecipient)
	}
	if request.From == "" {
		m.logger.Error("failed to handle message", "error", ErrNoSender)
		return fmt.Errorf("failed to handle message: %w", ErrNoSender)
	}
	if request.Body == "" {
		m.logger.Error("failed to handle message", "error", ErrNoBody)
		return fmt.Errorf("failed to handle message: %w", ErrNoBody)
	}

	duplicate, err := m.isDuplicate(ctx, request.ID)
	if err != nil {
		m.logger.Error("failed to handle message, sending to dead letter queue", "error", ErrDuplicateId)
		return fmt.Errorf("faled to handle message, sending to dead letter queue: %w", ErrDuplicateId)
	}
	if duplicate {
		m.logger.Error("failed to handle message", ErrDuplicateId)
		return fmt.Errorf("failed to handle message: %w", ErrDuplicateId)
	}
	// Message is fine and not duplicate, so we send, or forward to a sending process
	err = m.validateEmail(ctx, request)
	if err != nil {
		m.logger.Error("failed to handle message", "request ID", request.ID, "error", err)
		return fmt.Errorf("failed to handle message: %w", err)
	}

	err = m.sendEmail(ctx, request)
	if err != nil {
		m.logger.Error("failed to handle message", "request ID", request.ID, "error", err)
		return fmt.Errorf("failed to handle message %w", err)
	}

	return nil
}

func (m *Mailer) validateEmail(ctx context.Context, request EmailRequest) error {
	if ctx.Err() != nil {
		m.logger.Error("failed to validiate email", "request ID", request.ID, "error", ctx.Err())
		return fmt.Errorf("failet to validate email: %w", ctx.Err())
	}
	_, err := mail.ParseAddress(request.From)
	if err != nil {
		m.logger.Error("failed to validate email", "request ID", request.ID, "email", request.From, "error", err)
		return fmt.Errorf("failed to validate 'From' email: %w", err)
	}
	_, err = mail.ParseAddress(request.To)
	if err != nil {
		m.logger.Error("failed to validate email", "request ID", request.ID, "email", request.To, "error", err)
		return fmt.Errorf("failed to validate 'To' email: %w", err)
	}

	return nil
}

func (m *Mailer) sendEmail(ctx context.Context, request EmailRequest) error { //WIP
	if ctx.Err() != nil {
		m.logger.Error("failed to send email", "request ID", request.ID)
	}
	msg := gomail.NewMessage()
	msg.SetHeader("From", request.From) // This will be the email we set up for our Mailer Service
	msg.SetHeader("To", request.To)
	msg.SetHeader("Subject", request.Subject)
	msg.SetBody("text/html", request.Body)

	if request.Attachment.Filename != "" {
		msg.Attach(request.Attachment.Filename, gomail.SetCopyFunc(func(w io.Writer) error {
			_, err := w.Write([]byte(request.Attachment.Content))
			if err != nil {
				m.logger.Error("failed to send email", "request ID", request.ID, "error", err)
				err = fmt.Errorf("failed to send mail: %w", err)
			}
			return err
		}))
	}

	d := gomail.NewDialer(m.cfg.MailerHost, m.cfg.MailerPort, m.cfg.MailerUsername, m.cfg.MailerPassword)

	// send the email
	maxAttempts := 10

	err := m.emailAttemptLoop(ctx, 1, maxAttempts, msg, d)
	if err != nil {
		m.logger.Error("failed to send email", "request ID", request.ID, "error", err)
		// check error type to see if we need to forward to DLQ
		if errors.Is(err, ErrMaxSendAttemptsReached) {
			deadLetter := DeadLetter{
				OriginalMessage: request,
				FailureReason:   err.Error(),
				Attempts:        maxAttempts,
				FailedAt:        time.Now(),
			}
			m.logger.Info("forwarding email to dead letter queue", "request ID", request.ID, "attempts")
			err := m.addToDeadLetterQueue(ctx, deadLetter)
			if err != nil {
				m.logger.Error("failed housekeeping after failed send attempt", "request ID", request.ID, "error", err)
				return fmt.Errorf("failed housekeeping after failed send attempt %w", err)
			}
		}
	}
	m.logger.Info("successfully sent email", "request ID", request.ID)
	return nil
}

func (m *Mailer) emailAttemptLoop(ctx context.Context, attempt int, maxTries int, msg *gomail.Message, d *gomail.Dialer) error {
	if ctx.Err() != nil {
		m.logger.Error("email not sent", "error", ctx.Err())
		return fmt.Errorf("email not sent %w", ctx.Err())
	}
	err := d.DialAndSend(msg)
	attempt++
	if err != nil {
		if attempt < maxTries {
			m.emailAttemptLoop(ctx, attempt, maxTries, msg, d)
		}
		m.logger.Error("email not sent - maximum send attempts reached.")

		return ErrMaxSendAttemptsReached
	}
	return nil
}

// Checks against the database
func (m *Mailer) isDuplicate(ctx context.Context, ID string) (bool, error) {
	if ctx.Err() != nil {
		m.logger.Error("failed to check for duplicate email id", "error", ctx.Err())
		return false, fmt.Errorf("failed to check for duplicate email id %w", ctx.Err())
	}
	duplicate, err := m.db.IsDuplicate(ctx, ID)
	if err != nil {
		m.logger.Error("failed to check for duplicate email id", "error", err)
		return false, fmt.Errorf("failed to check for duplicate email id %w", err)
	}
	if duplicate {
		return true, nil
	}
	return false, nil
}

func (m *Mailer) addToDeadLetterQueue(ctx context.Context, deadLetter DeadLetter) error {
	// Add problem email to dead letter queue for later investigation
	return nil
}
