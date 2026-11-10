package processing

import "time"

type EmailRequest struct { // This type will need to be exported and used by all our other services
	Service    string     `json:"service"`
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
	OriginalMessage EmailRequest `json:"original_message"`
	FailureReason   string       `json:"failure_reason"`
	Attempts        int          `json:"attempts"`
	FailedAt        time.Time    `json:"failed_at"`
}
