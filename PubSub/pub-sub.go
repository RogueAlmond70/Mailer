package pubSub

import (
	"context"
	"fmt"

	"github.com/Mailer/internal"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const service = "MAILER"

func Setup(logger internal.Logger, ctx context.Context) (jetstream.Stream, error) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		logger.Info("failed to create nats connection", "error", err, "service", service)
		nc.Close()
		return nil, fmt.Errorf("failed to create nats connection: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		js.Conn().Close()
		return nil, fmt.Errorf("failed to create Jetstream: %v", err)
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     service,
		Subjects: []string{"mailer.*"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		// use an Errors.Is to check if its because the stream already exists - because if so, that's fine
		return nil, fmt.Errorf("failed to create stream: %v", err)
	}

	return stream, nil
}
