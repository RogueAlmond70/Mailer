package main

import (
	"context"
	"log/slog"
	"os"

	pubSub "github.com/Mailer/PubSub"
	"github.com/Mailer/internal"
	"github.com/Mailer/processing"
	"github.com/Mailer/storage"
)

func main() {
	ctx := context.Background()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	js, err := pubSub.Setup(logger, ctx)
	if err != nil {
		logger.Error("failed to setup nats jetstream", "error", err)
		os.Exit(2)
	}

	cfg, err := internal.LoadConfig()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(2)
	}

	db, err := storage.ConnectToDB()
	if err != nil {
		logger.Error("failed to create database connection", "error", err)
		os.Exit(2)
	}

	mailer := processing.CreateMailer(ctx, logger, *cfg, db, js)

	// Set up listen group for jetstream with subject "mailer"
	// Create child context with 60 second lifespan

	// use handler mailer.handleMessage()

}
