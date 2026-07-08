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

	SegmentSweepInterval       time.Duration
	SegmentSweepBatchSize      int
	SegmentSweepPerTenantCap   int
	SegmentSweepReclaimTimeout time.Duration
	SegmentSweepSafetyBatch    int
	SegmentSweepBackoffBase    time.Duration
	SegmentSweepBackoffCap     time.Duration
	SegmentSweepMaxAttempts    int

	BehaviorRetention         time.Duration
	BehaviorRetentionInterval time.Duration

	SeedJobInterval       time.Duration
	SeedJobPagesPerClaim  int
	SeedJobReclaimTimeout time.Duration

	// JourneyEnrollmentRetention prunes terminal (completed/exited) journey_enrollment
	// rows older than this; JourneyRetentionInterval is the sweep cadence.
	JourneyEnrollmentRetention time.Duration
	JourneyRetentionInterval   time.Duration

	RateLimitRPS     float64
	RateLimitBurst   int
	CircuitThreshold int
	CircuitWindow    time.Duration
	CircuitCooldown  time.Duration

	// CORSAllowedOrigins is the list of origins browsers are allowed to call
	// from. Empty = no cross-origin requests. Set CORS_ALLOWED_ORIGINS=* only
	// for a fully public, credential-free deployment.
	CORSAllowedOrigins []string
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

		SegmentSweepInterval:       getEnvDuration("SEGMENT_SWEEP_INTERVAL", 5*time.Second),
		SegmentSweepBatchSize:      getEnvInt("SEGMENT_SWEEP_BATCH_SIZE", 100),
		SegmentSweepPerTenantCap:   getEnvInt("SEGMENT_SWEEP_PER_TENANT_CAP", 50),
		SegmentSweepReclaimTimeout: getEnvDuration("SEGMENT_SWEEP_RECLAIM_TIMEOUT", time.Minute),
		SegmentSweepSafetyBatch:    getEnvInt("SEGMENT_SWEEP_SAFETY_BATCH", 20),
		SegmentSweepBackoffBase:    getEnvDuration("SEGMENT_SWEEP_BACKOFF_BASE", 30*time.Second),
		SegmentSweepBackoffCap:     getEnvDuration("SEGMENT_SWEEP_BACKOFF_CAP", time.Hour),
		SegmentSweepMaxAttempts:    getEnvInt("SEGMENT_SWEEP_MAX_ATTEMPTS", 10),

		BehaviorRetention:         getEnvDuration("BEHAVIOR_RETENTION", 40*24*time.Hour),
		BehaviorRetentionInterval: getEnvDuration("BEHAVIOR_RETENTION_INTERVAL", 6*time.Hour),

		SeedJobInterval:       getEnvDuration("SEED_JOB_INTERVAL", 5*time.Second),
		SeedJobPagesPerClaim:  getEnvInt("SEED_JOB_PAGES_PER_CLAIM", 10),
		SeedJobReclaimTimeout: getEnvDuration("SEED_JOB_RECLAIM_TIMEOUT", time.Minute),

		JourneyEnrollmentRetention: getEnvDuration("JOURNEY_ENROLLMENT_RETENTION", 90*24*time.Hour),
		JourneyRetentionInterval:   getEnvDuration("JOURNEY_RETENTION_INTERVAL", 6*time.Hour),

		RateLimitRPS:       getEnvFloat("RATE_LIMIT_RPS", 50),
		RateLimitBurst:     getEnvInt("RATE_LIMIT_BURST", 100),
		CircuitThreshold:   getEnvInt("CIRCUIT_THRESHOLD", 5),
		CircuitWindow:      getEnvDuration("CIRCUIT_WINDOW", time.Minute),
		CircuitCooldown:    getEnvDuration("CIRCUIT_COOLDOWN", 30*time.Second),
		CORSAllowedOrigins: splitCSV(os.Getenv("CORS_ALLOWED_ORIGINS")),
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

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
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
