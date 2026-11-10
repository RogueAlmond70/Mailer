package internal

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

type Config struct {
	MailerHost        string
	MailerPort        int
	MailerUsername    string
	MailerPassword    string
	PostgresHost      string
	PostgresPort      int
	PostgresUser      string
	PostgresPassword  string
	PostgresDbName    string
	PostgresSSLMode   string
	NATSURL           string
	MetricsAddr       string
	MaxSendAttempts   int
	MaxInFlight       int
	MaxDeliver        int
	RetryBackoff      time.Duration
	RedeliveryDelay   time.Duration
	ProcessingTimeout time.Duration
	AckWait           time.Duration
}

func LoadConfig() (*Config, error) {
	mailerPort, err := strconv.Atoi(getEnv("MAILER_PORT", "587"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse MAILER_PORT: %w", err)
	}

	postgresPort, err := strconv.Atoi(getEnv("POSTGRES_PORT", "5432"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse POSTGRES_PORT: %w", err)
	}

	maxSendAttempts, err := strconv.Atoi(getEnv("MAX_SEND_ATTEMPTS", "3"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse MAX_SEND_ATTEMPTS: %w", err)
	}
	if maxSendAttempts < 1 {
		return nil, fmt.Errorf("MAX_SEND_ATTEMPTS must be at least 1")
	}

	maxInFlight, err := strconv.Atoi(getEnv("MAX_IN_FLIGHT", "8"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse MAX_IN_FLIGHT: %w", err)
	}
	if maxInFlight < 1 {
		return nil, fmt.Errorf("MAX_IN_FLIGHT must be at least 1")
	}

	maxDeliver, err := strconv.Atoi(getEnv("MAX_DELIVER", "5"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse MAX_DELIVER: %w", err)
	}
	if maxDeliver < 1 {
		return nil, fmt.Errorf("MAX_DELIVER must be at least 1")
	}

	retryBackoff, err := time.ParseDuration(getEnv("RETRY_BACKOFF", "200ms"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse RETRY_BACKOFF: %w", err)
	}
	redeliveryDelay, err := time.ParseDuration(getEnv("REDELIVERY_DELAY", "2s"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse REDELIVERY_DELAY: %w", err)
	}
	processingTimeout, err := time.ParseDuration(getEnv("PROCESSING_TIMEOUT", "45s"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse PROCESSING_TIMEOUT: %w", err)
	}
	ackWait, err := time.ParseDuration(getEnv("ACK_WAIT", "90s"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ACK_WAIT: %w", err)
	}

	return &Config{
		MailerHost:        getEnv("MAILER_HOST", ""),
		MailerPort:        mailerPort,
		MailerUsername:    getEnv("MAILER_USERNAME", ""),
		MailerPassword:    getEnv("MAILER_PASSWORD", ""),
		PostgresHost:      getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:      postgresPort,
		PostgresUser:      getEnv("POSTGRES_USER", ""),
		PostgresPassword:  getEnv("POSTGRES_PASSWORD", ""),
		PostgresDbName:    getEnv("POSTGRES_DB_NAME", ""),
		PostgresSSLMode:   getEnv("POSTGRES_SSL_MODE", "disable"),
		NATSURL:           getEnv("NATS_URL", "nats://127.0.0.1:4222"),
		MetricsAddr:       getEnv("METRICS_ADDR", ":8080"),
		MaxSendAttempts:   maxSendAttempts,
		MaxInFlight:       maxInFlight,
		MaxDeliver:        maxDeliver,
		RetryBackoff:      retryBackoff,
		RedeliveryDelay:   redeliveryDelay,
		ProcessingTimeout: processingTimeout,
		AckWait:           ackWait,
	}, nil
}
