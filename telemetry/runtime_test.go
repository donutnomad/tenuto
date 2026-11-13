package telemetry

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func TestDisabledRuntimeKeepsStructuredLogsAndShutsDown(t *testing.T) {
	var output bytes.Buffer
	runtime, err := New(context.Background(), Config{
		Enabled:   false,
		LogOutput: &output,
	})
	if err != nil {
		t.Fatal(err)
	}
	providers := runtime.Providers()
	providers.Logger.InfoContext(context.Background(), "telemetry disabled", "component", "test")
	if text := output.String(); !strings.Contains(text, `"msg":"telemetry disabled"`) || !strings.Contains(text, `"component":"test"`) {
		t.Fatalf("structured log = %q", text)
	}
	if providers.TracerProvider == nil || providers.MeterProvider == nil || providers.LoggerProvider == nil || providers.Propagator == nil {
		t.Fatalf("disabled providers are incomplete: %+v", providers)
	}
	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestConfigDefaultsAndValidation(t *testing.T) {
	config := (Config{}).WithDefaults()
	if config.ServiceName != DefaultServiceName || config.TraceSampleRatio != 1 {
		t.Fatalf("service/ratio = %q/%v", config.ServiceName, config.TraceSampleRatio)
	}
	if config.MetricExportInterval != time.Minute || config.ShutdownTimeout != 5*time.Second {
		t.Fatalf("metric/shutdown = %v/%v", config.MetricExportInterval, config.ShutdownTimeout)
	}
	if err := config.Validate(); err != nil {
		t.Fatal(err)
	}
	config.TraceSampleRatio = 1.1
	if err := config.Validate(); err == nil {
		t.Fatal("trace sample ratio above one was accepted")
	}
	config.TraceSampleRatio = math.NaN()
	if err := config.Validate(); err == nil {
		t.Fatal("NaN trace sample ratio was accepted")
	}
}
