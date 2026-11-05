package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

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
	Cc         string     `json:"cc,omitempty"`
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

// This returns the string attachment type, the attachment decoded from base64 to its original type, and an error
func processAttachment(ctx context.Context, logger internal.Logger, attachment Attachment) (string, any, error) {
	if ctx.Err != nil {
		logger.Error("failed to process attachment", "error", ctx.Err)
		return "", -1, fmt.Errorf("failed to process attachment: %w", ctx.Err())
	}

}

func handleMessage(ctx context.Context, logger internal.Logger, DB internal.Database, msg *nats.Msg) error {
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

	duplicate, err := IsDuplicate(ctx, logger, request.ID, DB)
	if err != nil {
		logger.Error("failed to handle message, sending to dead letter queue", "error", ErrDuplicateId)
		return fmt.Errorf("faled to handle message, sending to dead letter queue: %w", ErrDuplicateId)
	}
	if duplicate {
		logger.Error("failed to handle message", ErrDuplicateId)
		return fmt.Errorf("failed to handle message: %w", ErrDuplicateId)
	}
	// Message is fine and not duplicate, so we send, or forward to a sending process

	return nil
}

func validateEmail(ctx context.Context, request EmailRequest) error {
	// Implement email validation

	return nil
}

func SendEmail(ctx context.Context, logger internal.Logger, request EmailRequest) error { //WIP
	m := gomail.NewMessage()
	m.SetHeader("From", request.From)
	m.SetHeader("To", request.To)
	m.SetHeader("Subject")
	m.SetBody("text/html", request.Body)

	if request.Attachment.Filename != "" {
		m.Attach(request.Attachment.Filename, gomail.SetCopyFunc(func(w io.Writer) error {
			_, err := w.Write([]byte(request.Attachment.Content))
			if err != nil {
				logger.Error("failed to send mail", "message ID", request.ID, "error", err)
				err = fmt.Errorf("failed to send mail: %w", err)
			}
			return err
		}))
	}

	d := gomail.NewDialer("smptp.example.com", 587, "user", "123445")

	// send the email
	err := d.DialAndSend(m)
	if err != nil {
		logger.Error("failed to send email", "message ID", request.ID, "error", err)
		return fmt.Errorf("failed to send email: %w", err)
	}

	return nil
}

// Checks against the database
func IsDuplicate(ctx context.Context, logger internal.Logger, ID string, DB internal.Database) (bool, error) {
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
