package telemetry

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strings"
	"time"
)

const (
	DefaultServiceName          = "tenuto"
	DefaultMetricExportInterval = time.Minute
	DefaultShutdownTimeout      = 5 * time.Second
)

// Config controls the process-wide OpenTelemetry providers.
type Config struct {
	Enabled              bool
	ServiceName          string
	ServiceVersion       string
	ServiceInstanceID    string
	TraceSampleRatio     float64
	MetricExportInterval time.Duration
	LogLevel             slog.Level
	ShutdownTimeout      time.Duration
	LogOutput            io.Writer
}

func (c Config) WithDefaults() Config {
	if strings.TrimSpace(c.ServiceName) == "" {
		c.ServiceName = DefaultServiceName
	}
	if c.TraceSampleRatio == 0 {
		c.TraceSampleRatio = 1
	}
	if c.MetricExportInterval == 0 {
		c.MetricExportInterval = DefaultMetricExportInterval
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = DefaultShutdownTimeout
	}
	if c.LogOutput == nil {
		c.LogOutput = os.Stdout
	}
	return c
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.ServiceName) == "" {
		return fmt.Errorf("telemetry service name is empty")
	}
	if math.IsNaN(c.TraceSampleRatio) || math.IsInf(c.TraceSampleRatio, 0) || c.TraceSampleRatio <= 0 || c.TraceSampleRatio > 1 {
		return fmt.Errorf("telemetry trace sample ratio must be greater than 0 and at most 1")
	}
	if c.MetricExportInterval <= 0 {
		return fmt.Errorf("telemetry metric export interval must be positive")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("telemetry shutdown timeout must be positive")
	}
	return nil
}
