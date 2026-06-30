// Command migrate applies or rolls back database migrations.
//
// Usage:
//
//	migrate up
//	migrate down
package main

import (
	"fmt"
	"os"

	"github.com/dinhphu28/osscdp/internal/config"
	"github.com/dinhphu28/osscdp/internal/platform/migrate"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}

	direction := "up"
	if len(os.Args) > 1 {
		direction = os.Args[1]
	}

	switch direction {
	case "up":
		err = migrate.Up(cfg.DatabaseURL)
	case "down":
		err = migrate.Down(cfg.DatabaseURL)
	default:
		fmt.Fprintln(os.Stderr, "unknown direction:", direction, "(use up|down)")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	fmt.Println("migrate", direction, "complete")
}
