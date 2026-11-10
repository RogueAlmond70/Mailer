package main

import (
	"context"
	"log/slog"
	"os"

	pubSub "github.com/Mailer/PubSub"
	"github.com/Mailer/internal"
	"github.com/Mailer/processing"
	"github.com/Mailer/storage"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	stream, js, err := pubSub.Setup(logger, ctx)
	if err != nil {
		logger.Error("failed to setup nats jetstream", "error", err)
		os.Exit(2)
	}

	cfg, err := internal.LoadConfig()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(2)
	}

	db, err := storage.ConnectToDB(ctx, logger, *cfg)
	if err != nil {
		logger.Error("failed to create database connection", "error", err)
		os.Exit(2)
	}

	mailer := processing.CreateMailer(ctx, logger, *cfg, db, stream, js)

	// Set up listen group for jetstream with subject "mailer"

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "mailer-worker",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    5,
		FilterSubject: "mailer.send",
	})

	_, err = consumer.Consume(func(msg jetstream.Msg) {
		logger.Info("Received:", "message", msg.Subject())

		err := mailer.HandleMessage(ctx, msg.Data())
		if err != nil {
			logger.Error("failed to consume message", "error", err)
			msg.Nak()
			return
		}
		msg.Ack()
	})
	if err != nil {
		logger.Error("failed to create consumer for jetstream", "error", err)
		os.Exit(2)
	}

}
