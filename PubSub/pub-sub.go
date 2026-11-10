package pubSub

import (
	"context"
	"fmt"
	"time"

	"github.com/Mailer/internal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const streamName = "MAILER"

func Setup(ctx context.Context, natsURL string) (*nats.Conn, jetstream.JetStream, jetstream.Stream, error) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create nats connection: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, nil, fmt.Errorf("failed to create jetstream: %w", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"email.*"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		nc.Close()
		return nil, nil, nil, fmt.Errorf("failed to create stream: %w", err)
	}

	return nc, js, stream, nil
}

// EnsureConsumer creates the shared durable consumer. Every replica uses this
// name, so JetStream delivers each email.send payload to one pod.
func EnsureConsumer(ctx context.Context, stream jetstream.Stream, cfg internal.Config) (jetstream.Consumer, error) {
	ackWait := cfg.AckWait
	if ackWait <= 0 {
		ackWait = 90 * time.Second
	}
	maxDeliver := cfg.MaxDeliver
	if maxDeliver < 1 {
		maxDeliver = 5
	}
	maxInFlight := cfg.MaxInFlight
	if maxInFlight < 1 {
		maxInFlight = 8
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "mailer-worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    maxDeliver,
		FilterSubject: internal.SubjectSend,
		MaxAckPending: maxInFlight,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	return consumer, nil
}

// JetStreamPublisher publishes outcomes onto the mailer stream.
type JetStreamPublisher struct {
	JS jetstream.JetStream
}

func (p JetStreamPublisher) Publish(ctx context.Context, subject string, payload []byte) error {
	if _, err := p.JS.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}
