package internal

import (
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	t.Setenv("MAILER_HOST", "smtp.example.com")
	t.Setenv("MAILER_PORT", "2525")
	t.Setenv("MAILER_USERNAME", "mailer")
	t.Setenv("MAILER_PASSWORD", "secret")
	t.Setenv("POSTGRES_HOST", "db.example.com")
	t.Setenv("POSTGRES_PORT", "5433")
	t.Setenv("POSTGRES_USER", "mailer")
	t.Setenv("POSTGRES_PASSWORD", "secret")
	t.Setenv("POSTGRES_DB_NAME", "mailer")
	t.Setenv("POSTGRES_SSL_MODE", "require")
	t.Setenv("NATS_URL", "nats://nats:4222")
	t.Setenv("METRICS_ADDR", ":9090")
	t.Setenv("MAX_SEND_ATTEMPTS", "4")
	t.Setenv("MAX_IN_FLIGHT", "2")
	t.Setenv("MAX_DELIVER", "6")
	t.Setenv("RETRY_BACKOFF", "10ms")
	t.Setenv("REDELIVERY_DELAY", "3s")
	t.Setenv("PROCESSING_TIMEOUT", "8s")
	t.Setenv("ACK_WAIT", "20s")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.MailerHost != "smtp.example.com" || cfg.MailerPort != 2525 {
		t.Fatalf("smtp config = %s:%d", cfg.MailerHost, cfg.MailerPort)
	}
	if cfg.PostgresHost != "db.example.com" || cfg.PostgresPort != 5433 || cfg.PostgresSSLMode != "require" {
		t.Fatalf("postgres config = %+v", cfg)
	}
	if cfg.NATSURL != "nats://nats:4222" || cfg.MetricsAddr != ":9090" {
		t.Fatalf("nats/metrics = %s %s", cfg.NATSURL, cfg.MetricsAddr)
	}
	if cfg.MaxSendAttempts != 4 || cfg.MaxInFlight != 2 || cfg.MaxDeliver != 6 {
		t.Fatalf("limits = %+v", cfg)
	}
	if cfg.RetryBackoff != 10*time.Millisecond || cfg.RedeliveryDelay != 3*time.Second || cfg.ProcessingTimeout != 8*time.Second || cfg.AckWait != 20*time.Second {
		t.Fatalf("durations = %+v", cfg)
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	t.Setenv("MAX_SEND_ATTEMPTS", "0")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected MAX_SEND_ATTEMPTS error")
	}

	t.Setenv("MAX_SEND_ATTEMPTS", "3")
	t.Setenv("RETRY_BACKOFF", "soon")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected RETRY_BACKOFF error")
	}
}
