package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSettleAcksNilAndNaksErrors(t *testing.T) {
	acked := &fakeMsg{}
	if err := settle(acked, time.Second, nil); err != nil {
		t.Fatal(err)
	}
	if !acked.acked || acked.nacked {
		t.Fatalf("ack path = %+v", acked)
	}

	nacked := &fakeMsg{}
	if err := settle(nacked, 2*time.Second, errors.New("db down")); err != nil {
		t.Fatal(err)
	}
	if nacked.acked || !nacked.nacked || nacked.delay != 2*time.Second {
		t.Fatalf("nak path = %+v", nacked)
	}
}

type fakeMsg struct {
	acked  bool
	nacked bool
	delay  time.Duration
}

func (f *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return &jetstream.MsgMetadata{}, nil }
func (f *fakeMsg) Data() []byte                              { return nil }
func (f *fakeMsg) Headers() nats.Header                      { return nil }
func (f *fakeMsg) Subject() string                           { return "email.send" }
func (f *fakeMsg) Reply() string                             { return "" }
func (f *fakeMsg) Ack() error                                { f.acked = true; return nil }
func (f *fakeMsg) DoubleAck(context.Context) error           { return f.Ack() }
func (f *fakeMsg) Nak() error                                { f.nacked = true; return nil }
func (f *fakeMsg) NakWithDelay(delay time.Duration) error {
	f.nacked = true
	f.delay = delay
	return nil
}
func (f *fakeMsg) InProgress() error           { return nil }
func (f *fakeMsg) Term() error                 { return nil }
func (f *fakeMsg) TermWithReason(string) error { return nil }
