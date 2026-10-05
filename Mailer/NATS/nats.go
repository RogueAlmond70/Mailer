package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

const service = "MAILER"

func Setup(logger zap.Logger, ctx context.Context) (jetstream.Stream, error) {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		logger.Info("failed to create nats connection", "error", err, "service", service)
		return nil, fmt.Errorf("error creating nats connection: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("error creating Jetstream: %v", err)
	}

	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     service,
		Subjects: []string{"mailer.*"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating stream: %v", err)
	}

	return stream, nil
}
