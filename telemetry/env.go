package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// ConfigFromEnv applies Tenuto's telemetry environment variables to base.
func ConfigFromEnv(base Config) (Config, error) {
	return configFromLookup(base, os.LookupEnv)
}

// NewFromEnv creates process telemetry from a base config and environment overrides.
func NewFromEnv(ctx context.Context, base Config) (*Runtime, error) {
	config, err := ConfigFromEnv(base)
	if err != nil {
		return nil, err
	}
	return New(ctx, config)
}

type envLookup func(string) (string, bool)

func configFromLookup(base Config, lookup envLookup) (Config, error) {
	config := base.WithDefaults()
	if value := envValue(lookup, "OTEL_SERVICE_NAME"); value != "" {
		config.ServiceName = value
	}
	if raw := envValue(lookup, "TENUTO_OTEL_ENABLED"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse TENUTO_OTEL_ENABLED: %w", err)
		}
		config.Enabled = value
	}
	if raw := envValue(lookup, "TENUTO_OTEL_TRACE_SAMPLE_RATIO"); raw != "" {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > 1 {
			return Config{}, fmt.Errorf("TENUTO_OTEL_TRACE_SAMPLE_RATIO must be greater than 0 and at most 1")
		}
		config.TraceSampleRatio = value
	}
	if raw := envValue(lookup, "TENUTO_OTEL_METRIC_EXPORT_INTERVAL"); raw != "" {
		value, err := parsePositiveDuration("TENUTO_OTEL_METRIC_EXPORT_INTERVAL", raw)
		if err != nil {
			return Config{}, err
		}
		config.MetricExportInterval = value
	}
	if raw := envValue(lookup, "TENUTO_OTEL_SHUTDOWN_TIMEOUT"); raw != "" {
		value, err := parsePositiveDuration("TENUTO_OTEL_SHUTDOWN_TIMEOUT", raw)
		if err != nil {
			return Config{}, err
		}
		config.ShutdownTimeout = value
	}
	if raw := envValue(lookup, "TENUTO_OTEL_LOG_LEVEL"); raw != "" {
		var level slog.Level
		if err := level.UnmarshalText([]byte(strings.ToUpper(raw))); err != nil {
			return Config{}, fmt.Errorf("parse TENUTO_OTEL_LOG_LEVEL: %w", err)
		}
		config.LogLevel = level
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func envValue(lookup envLookup, name string) string {
	value, ok := lookup(name)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func parsePositiveDuration(name, raw string) (time.Duration, error) {
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return value, nil
}
