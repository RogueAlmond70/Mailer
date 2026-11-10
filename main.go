package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pubSub "github.com/Mailer/PubSub"
	"github.com/Mailer/internal"
	"github.com/Mailer/metrics"
	"github.com/Mailer/processing"
	"github.com/Mailer/storage"
	"github.com/Mailer/worker"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg, err := internal.LoadConfig()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		return 2
	}
	if cfg.MailerHost == "" {
		logger.Warn("MAILER_HOST is empty; sends will fail until it is set")
	}

	nc, js, stream, err := pubSub.Setup(ctx, cfg.NATSURL)
	if err != nil {
		logger.Error("failed to setup nats jetstream", "error", err)
		return 2
	}
	defer nc.Drain()

	db, err := storage.ConnectToDB(ctx, logger, *cfg)
	if err != nil {
		logger.Error("failed to create database connection", "error", err)
		return 2
	}
	defer db.Close()

	appMetrics := metrics.New()
	mailer := processing.CreateMailer(logger, *cfg, db, pubSub.JetStreamPublisher{JS: js}, processing.SMTPSender{
		Host:     cfg.MailerHost,
		Port:     cfg.MailerPort,
		Username: cfg.MailerUsername,
		Password: cfg.MailerPassword,
	}, appMetrics)

	consumer, err := pubSub.EnsureConsumer(ctx, stream, *cfg)
	if err != nil {
		logger.Error("failed to create consumer", "error", err)
		return 2
	}

	srv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           appMetrics.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "error", err)
			stop()
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("failed to shut down metrics server", "error", err)
		}
	}()

	logger.Info("mailer listening", "subject", internal.SubjectSend, "metrics", cfg.MetricsAddr)

	if err := worker.Run(ctx, logger, consumer, appMetrics, worker.Options{
		MaxInFlight:       cfg.MaxInFlight,
		ProcessingTimeout: cfg.ProcessingTimeout,
		RedeliveryDelay:   cfg.RedeliveryDelay,
	}, mailer.HandleMessage); err != nil {
		logger.Error("consumer stopped", "error", err)
		return 2
	}
	return 0
}
