package processing

import (
	"context"
	"errors"
	"io"
	"net/textproto"
	"strconv"
	"strings"

	"gopkg.in/gomail.v2"
)

// Sender delivers one email. Permanent failures should be wrapped with Permanent.
type Sender interface {
	Send(ctx context.Context, request EmailRequest) error
}

// SMTPSender delivers mail with gomail.
type SMTPSender struct {
	Host     string
	Port     int
	Username string
	Password string
}

func (s SMTPSender) Send(ctx context.Context, request EmailRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	msg := gomail.NewMessage()
	msg.SetHeader("From", request.From)
	msg.SetHeader("To", request.To)
	msg.SetHeader("Subject", request.Subject)
	msg.SetBody("text/html", request.Body)

	if request.Attachment.Filename != "" {
		copyBody := gomail.SetCopyFunc(func(w io.Writer) error {
			_, err := w.Write(request.Attachment.Content)
			return err
		})
		if request.Attachment.Inline {
			msg.Embed(request.Attachment.Filename, copyBody)
		} else {
			msg.Attach(request.Attachment.Filename, copyBody)
		}
	}

	d := gomail.NewDialer(s.Host, s.Port, s.Username, s.Password)
	if err := d.DialAndSend(msg); err != nil {
		return classifySendError(err)
	}
	return nil
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

// Permanent marks a provider failure that will not succeed on retry.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err, or an error it wraps, should not be retried.
func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

func classifySendError(err error) error {
	var textErr *textproto.Error
	if errors.As(err, &textErr) && textErr.Code >= 500 {
		return Permanent(err)
	}
	if code := smtpCode(err.Error()); code >= 500 {
		return Permanent(err)
	}
	return err
}

func smtpCode(msg string) int {
	for _, field := range strings.Fields(msg) {
		field = strings.Trim(field, ":;,.")
		if len(field) < 3 {
			continue
		}
		code, err := strconv.Atoi(field[:3])
		if err != nil || code < 400 || code > 599 {
			continue
		}
		if strconv.Itoa(code) == field[:3] {
			return code
		}
	}
	return 0
}
