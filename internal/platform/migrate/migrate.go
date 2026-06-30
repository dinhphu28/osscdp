// Package migrate runs goose migrations from the embedded SQL files.
package migrate

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // register "pgx" database/sql driver
	"github.com/pressly/goose/v3"

	"github.com/dinhphu28/osscdp/migrations"
)

func openDB(url string) (*sql.DB, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, fmt.Errorf("open sql db: %w", err)
	}
	if err := goose.SetDialect("postgres"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set goose dialect: %w", err)
	}
	goose.SetBaseFS(migrations.FS)
	return db, nil
}

// Up applies all pending migrations.
func Up(url string) error {
	db, err := openDB(url)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := goose.Up(db, "."); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}

// Down rolls back the most recent migration.
func Down(url string) error {
	db, err := openDB(url)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := goose.Down(db, "."); err != nil {
		return fmt.Errorf("goose down: %w", err)
	}
	return nil
}
