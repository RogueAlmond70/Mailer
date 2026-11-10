package processing

import (
	"errors"
	"net/textproto"
	"testing"
)

func TestClassifySendError(t *testing.T) {
	if code := smtpCode("550 no such user"); code != 550 {
		t.Fatalf("code = %d", code)
	}
	if code := smtpCode("dial tcp 127.0.0.1:587: connection refused"); code != 0 {
		t.Fatalf("code = %d", code)
	}
	if !IsPermanent(classifySendError(errors.New("554 transaction failed"))) {
		t.Fatal("554 should be permanent")
	}
	if IsPermanent(classifySendError(errors.New("421 service not available"))) {
		t.Fatal("421 should be retried")
	}
	if IsPermanent(classifySendError(errors.New("dial tcp: connection refused"))) {
		t.Fatal("network errors should be retried")
	}
	textErr := &textproto.Error{Code: 552, Msg: "mailbox full"}
	if !IsPermanent(classifySendError(textErr)) {
		t.Fatal("textproto 552 should be permanent")
	}
}
