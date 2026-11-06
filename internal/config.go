package internal

import (
	"fmt"
	"os"
	"strconv"
)

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

type Config struct {
	MailerHost       string
	MailerPort       int
	MailerUsername   string
	MailerPassword   string
	PostgresUser     string
	PostgresPassword string
	PostgresDbName   string
	PostgresSSLMode  string
}

func LoadConfig() (*Config, error) {
	mailerPort, err := strconv.Atoi(getEnv("MAILER_PORT", "587"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse MAILER_PORT: %w", err)
	}

	return &Config{
		MailerHost:       getEnv("MAILER_HOST", ""),
		MailerPort:       mailerPort,
		MailerUsername:   getEnv("MAILER_USERNAME", ""),
		MailerPassword:   getEnv("MAILER_PASSWORD", ""),
		PostgresUser:     getEnv("POSTGRES_USER", ""),
		PostgresPassword: getEnv("POSTGRES_PASSWORD", ""),
		PostgresDbName:   getEnv("POSTGRES_DB_NAME", ""),
		PostgresSSLMode:  getEnv("POSTGRES_SSL_MODE", "disable"),
	}, nil
}
