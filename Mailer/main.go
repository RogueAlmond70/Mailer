package main

import (
	"context"
	"time"

	nats "github.com/Mailer/NATS"
	"go.uber.org/zap"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger, err := zap.NewProduction()
	if err != nil {
		// decide how you want to handle this error
	}
	defer logger.Sync() // Flush buggered log writes to file when program exits
	//logger.Info("failed to do thing - this is an illustration obvs", zap.String("url", url), zap.Int("attempt", 3), zap.Duration("backoff", time.Second))

	jetstream, err := nats.Setup(logger, ctx)

}
