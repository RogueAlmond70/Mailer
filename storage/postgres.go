package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Mailer/internal"
)

func ConnectToDB(ctx context.Context, logger internal.Logger, cfg internal.Config) (*sql.DB, error) {
	connStr := fmt.Sprintf("user=%v password=%v dbname=%v sslmode=%v", cfg.PostgresUser, cfg.PostgresPassword, cfg.PostgresDbName, cfg.PostgresSSLMode)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		logger.Error("failed to check database connection", "error", err)
		return nil, fmt.Errorf("failed to check database connection %w", err)
	}

	err = CreateTableIfNotExists(ctx, db)
	if err != nil {
		logger.Error("failed to create table", "error", err)
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	return db, err
}

type PostgresDB struct {
	DB *sql.DB
}

func (p *PostgresDB) IsDuplicate(ctx context.Context, request_id string) (bool, error) {
	// TODO: Implement me
	return false, nil
}

func (p *PostgresDB) StoreEmailRecord(ctx context.Context, data []byte) error {
	// TODO: Implement me
	return nil
}

func (p *PostgresDB) DeleteEmailRecord(ctx context.Context, request_id string) error {
	// TODO: Implement me
	return nil
}

func (p *PostgresDB) UploadDeadLetter(ctx context.Context, data []byte) error {
	// TODO: Implement me
	return nil
}

func CreateTableIfNotExists(ctx context.Context, db *sql.DB) error {
	// TODO: Implement me
	return nil
}
