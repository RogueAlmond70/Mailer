package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/mail"

	"github.com/Mailer/internal"
	"github.com/nats-io/nats.go"
	"gopkg.in/gomail.v2"
)

var (
	ErrNoId        = errors.New("ID not provided in email")
	ErrNoRecipient = errors.New("no email recipient")
	ErrNoSender    = errors.New("no sender")
	ErrNoTemplate  = errors.New("no email template")
	ErrNoBody      = errors.New("no email body")
	ErrDuplicateId = errors.New("duplicate email id")
)

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

func handleMessage(ctx context.Context, logger internal.Logger, DB internal.Database, msg *nats.Msg, cfg internal.Config) error {
	if ctx.Err() != nil {
		logger.Error("failed to handle message", "error", ctx.Err())
		return fmt.Errorf("failed to handle message %w", ctx.Err())
	}

	var request EmailRequest

	err := json.Unmarshal(msg.Data, &request)
	if err != nil {
		logger.Error("failed to unmarshal payload", "error", err)
		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	if request.ID == "" {
		logger.Error("failed to handle message", "error", ErrNoId)
		return fmt.Errorf("failed to handle message %w", ErrNoId)
	}
	if request.To == "" {
		logger.Error("failed to handle message", "error", ErrNoRecipient)
		return fmt.Errorf("failed to handle message: %w", ErrNoRecipient)
	}
	if request.From == "" {
		logger.Error("failed to handle message", "error", ErrNoSender)
		return fmt.Errorf("failed to handle message: %w", ErrNoSender)
	}
	if request.Body == "" {
		logger.Error("failed to handle message", "error", ErrNoBody)
		return fmt.Errorf("failed to handle message: %w", ErrNoBody)
	}

	duplicate, err := isDuplicate(ctx, logger, request.ID, DB)
	if err != nil {
		logger.Error("failed to handle message, sending to dead letter queue", "error", ErrDuplicateId)
		return fmt.Errorf("faled to handle message, sending to dead letter queue: %w", ErrDuplicateId)
	}
	if duplicate {
		logger.Error("failed to handle message", ErrDuplicateId)
		return fmt.Errorf("failed to handle message: %w", ErrDuplicateId)
	}
	// Message is fine and not duplicate, so we send, or forward to a sending process
	err = validateEmail(ctx, logger, request)
	if err != nil {
		logger.Error("failed to handle message", "request ID", request.ID, "error", err)
		return fmt.Errorf("failed to handle message: %w", err)
	}

	err = sendEmail(ctx, logger, request, cfg)
	if err != nil {
		logger.Error("failed to handle message", "request ID", request.ID, "error", err)
		return fmt.Errorf("failed to handle message %w", err)
	}

	return nil
}

func validateEmail(ctx context.Context, logger internal.Logger, request EmailRequest) error {
	if ctx.Err() != nil {
		logger.Error("failed to validiate email", "request ID", request.ID, "error", ctx.Err())
		return fmt.Errorf("failet to validate email: %w", ctx.Err())
	}
	_, err := mail.ParseAddress(request.From)
	if err != nil {
		logger.Error("failed to validate email", "request ID", request.ID, "email", request.From, "error", err)
		return fmt.Errorf("failed to validate 'From' email: %w", err)
	}
	_, err = mail.ParseAddress(request.To)
	if err != nil {
		logger.Error("failed to validate email", "request ID", request.ID, "email", request.To, "error", err)
		return fmt.Errorf("failed to validate 'To' email: %w", err)
	}

	return nil
}

func sendEmail(ctx context.Context, logger internal.Logger, request EmailRequest, cfg internal.Config) error { //WIP
	if ctx.Err() != nil {
		logger.Error("failed to send email", "request ID", request.ID)
	}
	m := gomail.NewMessage()
	m.SetHeader("From", request.From) // This will be the email we set up for our Mailer Service
	m.SetHeader("To", request.To)
	m.SetHeader("Subject", request.Subject)
	m.SetBody("text/html", request.Body)

	if request.Attachment.Filename != "" {
		m.Attach(request.Attachment.Filename, gomail.SetCopyFunc(func(w io.Writer) error {
			_, err := w.Write([]byte(request.Attachment.Content))
			if err != nil {
				logger.Error("failed to send mail", "request ID", request.ID, "error", err)
				err = fmt.Errorf("failed to send mail: %w", err)
			}
			return err
		}))
	}

	d := gomail.NewDialer(cfg.MailerHost, cfg.MailerPort, cfg.MailerUsername, cfg.MailerPassword)

	// send the email
	maxAttempts := 10

	for i := 0; i < maxAttempts; i++ {

	}
	// set retry counter
	retries := 0
	// Try to send
	err := d.DialAndSend(m)
	// If it fails, try again, checking each time for failure, until a max retry limit has been reached
	if err != nil {
		// check retry limit
		if retries < maxAttempts {

		}
	}

	err := d.DialAndSend(m)
	if err != nil {
		logger.Error("failed to send email", "request ID", request.ID, "error", err)
		return fmt.Errorf("failed to send email: %w", err)
	}

	return nil
}

func emailAttemptLoop(attempt int, maxTries int, m *gomail.Message, d *gomail.Dialer) {
	err := d.DialAndSend(m)
	attempt++
	if err != nil {
		if attempt < maxTries {
			emailAttemptLoop(attempt, maxTries, m, d)
		}

	}
	return nil
}

// Checks against the database
func isDuplicate(ctx context.Context, logger internal.Logger, ID string, DB internal.Database) (bool, error) {
	if ctx.Err() != nil {
		logger.Error("failed to check for duplicate email id", "error", ctx.Err())
		return false, fmt.Errorf("failed to check for duplicate email id %w", ctx.Err())
	}
	duplicate, err := DB.IsDuplicate(ctx, ID)
	if err != nil {
		logger.Error("failed to check for duplicate email id", "error", err)
		return false, fmt.Errorf("failed to check for duplicate email id %w", err)
	}
	if duplicate {
		return true, nil
	}
	return false, nil
}

func AddToDeadLetterQueue(ctx context.Context, logger internal.Logger, request EmailRequest) error {
	// Add problem email to dead letter queue for later investigation
	return nil
}
