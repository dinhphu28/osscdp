// Package config loads and validates process configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strings"
)

// Config holds all runtime configuration for the CDP processes.
type Config struct {
	DatabaseURL   string
	AdminAPIToken string
	HTTPAddr      string
	LogLevel      string
}

// Load reads configuration from environment variables, applies defaults, and
// fails fast when a required value is missing.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		AdminAPIToken: os.Getenv("ADMIN_API_TOKEN"),
		HTTPAddr:      getEnvDefault("HTTP_ADDR", ":8080"),
		LogLevel:      getEnvDefault("LOG_LEVEL", "info"),
	}

	var missing []string
	if cfg.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if cfg.AdminAPIToken == "" {
		missing = append(missing, "ADMIN_API_TOKEN")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	return cfg, nil
}

func getEnvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
