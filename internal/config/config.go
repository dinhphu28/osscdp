// Package config loads and validates process configuration from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the CDP processes.
type Config struct {
	DatabaseURL   string
	AdminAPIToken string
	EncryptionKey string
	HTTPAddr      string
	LogLevel      string

	// Event pipeline (cdp-worker).
	KafkaBrokers       []string
	KafkaConsumerGroup string
	MetricsAddr        string
	RelayPollInterval  time.Duration
	RelayBatchSize     int
	MaxRetries         int

	ActivationPollInterval time.Duration
	ActivationBatchSize    int
}

// Load reads configuration from environment variables, applies defaults, and
// fails fast when a required value is missing.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:        os.Getenv("DATABASE_URL"),
		AdminAPIToken:      os.Getenv("ADMIN_API_TOKEN"),
		EncryptionKey:      os.Getenv("CDP_ENCRYPTION_KEY"),
		HTTPAddr:           getEnvDefault("HTTP_ADDR", ":8080"),
		LogLevel:           getEnvDefault("LOG_LEVEL", "info"),
		KafkaBrokers:       splitCSV(getEnvDefault("KAFKA_BROKERS", "localhost:9092")),
		KafkaConsumerGroup: getEnvDefault("KAFKA_CONSUMER_GROUP", "cdp-worker"),
		MetricsAddr:        getEnvDefault("METRICS_ADDR", ":9100"),
		RelayPollInterval:  getEnvDuration("RELAY_POLL_INTERVAL", time.Second),
		RelayBatchSize:     getEnvInt("RELAY_BATCH_SIZE", 100),
		MaxRetries:         getEnvInt("MAX_RETRIES", 5),

		ActivationPollInterval: getEnvDuration("ACTIVATION_POLL_INTERVAL", 2*time.Second),
		ActivationBatchSize:    getEnvInt("ACTIVATION_BATCH_SIZE", 50),
	}

	var missing []string
	if cfg.DatabaseURL == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if cfg.AdminAPIToken == "" {
		missing = append(missing, "ADMIN_API_TOKEN")
	}
	if cfg.EncryptionKey == "" {
		missing = append(missing, "CDP_ENCRYPTION_KEY")
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

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
