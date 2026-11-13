package telemetry

import (
	"log/slog"
	"testing"
	"time"
)

func TestConfigFromEnvAppliesWorkerTelemetrySettings(t *testing.T) {
	values := map[string]string{
		"OTEL_SERVICE_NAME":                  "orders-worker",
		"TENUTO_OTEL_ENABLED":                "true",
		"TENUTO_OTEL_TRACE_SAMPLE_RATIO":     "0.25",
		"TENUTO_OTEL_METRIC_EXPORT_INTERVAL": "15s",
		"TENUTO_OTEL_LOG_LEVEL":              "DEBUG",
		"TENUTO_OTEL_SHUTDOWN_TIMEOUT":       "9s",
	}
	config, err := configFromLookup(Config{ServiceName: "fallback", ServiceInstanceID: "worker-1"}, mapLookup(values))
	if err != nil {
		t.Fatal(err)
	}
	if !config.Enabled || config.ServiceName != "orders-worker" || config.ServiceInstanceID != "worker-1" {
		t.Fatalf("identity/enabled = %q/%q/%v", config.ServiceName, config.ServiceInstanceID, config.Enabled)
	}
	if config.TraceSampleRatio != 0.25 || config.MetricExportInterval != 15*time.Second {
		t.Fatalf("ratio/metric interval = %v/%v", config.TraceSampleRatio, config.MetricExportInterval)
	}
	if config.LogLevel != slog.LevelDebug || config.ShutdownTimeout != 9*time.Second {
		t.Fatalf("log level/shutdown = %v/%v", config.LogLevel, config.ShutdownTimeout)
	}
}

func TestConfigFromEnvRejectsInvalidSettings(t *testing.T) {
	if _, err := configFromLookup(Config{}, mapLookup(map[string]string{
		"TENUTO_OTEL_TRACE_SAMPLE_RATIO": "NaN",
	})); err == nil {
		t.Fatal("NaN sample ratio was accepted")
	}
}

func mapLookup(values map[string]string) envLookup {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
