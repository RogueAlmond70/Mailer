package storage

import (
	"database/sql"
	"fmt"
)

func ConnectToDB() (*sql.DB, error) {
	connStr := "user=username password=password dbname=database sslmode=disable" // Move this to a config file, referencing AWS secrets location
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		// log first
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	err = createTableIfNotExists(db)
	if err != nil {
		return nil, fmt.Errorf("error creating table: %w", err) // same again
	}
	return db, err // err should be nil at this point, so I would rather return nil
}
