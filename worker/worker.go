package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Mailer/internal"
	"github.com/Mailer/metrics"
	"github.com/nats-io/nats.go/jetstream"
)

type Options struct {
	MaxInFlight       int
	ProcessingTimeout time.Duration
	RedeliveryDelay   time.Duration
}

// Handler processes the payload of one JetStream message.
// A non-nil error is negatively acknowledged. A nil error is acknowledged.
type Handler func(context.Context, []byte) error

// Run consumes until ctx is cancelled, then waits for messages already in flight.
func Run(ctx context.Context, logger internal.Logger, consumer jetstream.Consumer, m *metrics.Metrics, opts Options, handle Handler) error {
	if m == nil {
		m = metrics.New()
	}
	if opts.MaxInFlight < 1 {
		opts.MaxInFlight = 1
	}
	if opts.ProcessingTimeout <= 0 {
		opts.ProcessingTimeout = 45 * time.Second
	}

	sem := make(chan struct{}, opts.MaxInFlight)
	var wg sync.WaitGroup

	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			var handleErr error
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						handleErr = fmt.Errorf("handler panicked: %v", rec)
					}
				}()
				msgCtx, cancel := context.WithTimeout(context.Background(), opts.ProcessingTimeout)
				defer cancel()
				handleErr = handle(msgCtx, msg.Data())
			}()

			if settleErr := settle(msg, opts.RedeliveryDelay, handleErr); settleErr != nil {
				logger.Error("failed to settle message", "error", settleErr, "subject", msg.Subject())
			}
			if handleErr != nil {
				m.Nacked.Inc()
				logger.Error("message will be redelivered", "error", handleErr, "subject", msg.Subject())
				return
			}
			m.Acked.Inc()
		}()
	})
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	<-ctx.Done()
	cc.Stop()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(opts.ProcessingTimeout + time.Second):
		logger.Error("timed out waiting for in-flight messages")
	}
	return nil
}

func settle(msg jetstream.Msg, delay time.Duration, err error) error {
	if err != nil {
		if delay < 0 {
			delay = 0
		}
		return msg.NakWithDelay(delay)
	}
	return msg.Ack()
}
