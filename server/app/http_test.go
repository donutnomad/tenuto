package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHTTPHealthAndRuntimeReadiness(t *testing.T) {
	var runtimeCalls atomic.Int64
	runtime := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		runtimeCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	health := &healthState{}
	handler := newHTTPHandler(runtime, health, nil, nil)

	assertStatus(t, handler, http.MethodGet, "/health/live", http.StatusOK)
	assertStatus(t, handler, http.MethodGet, "/health/ready", http.StatusServiceUnavailable)
	assertStatus(t, handler, http.MethodPost, "/runtime/workflow/start", http.StatusServiceUnavailable)
	if runtimeCalls.Load() != 0 {
		t.Fatal("runtime handler received traffic before readiness")
	}

	health.ready.Store(true)
	assertStatus(t, handler, http.MethodGet, "/health/ready", http.StatusOK)
	assertStatus(t, handler, http.MethodPost, "/runtime/workflow/start", http.StatusNoContent)
	if runtimeCalls.Load() != 1 {
		t.Fatalf("runtime calls = %d", runtimeCalls.Load())
	}

	health.ready.Store(false)
	assertStatus(t, handler, http.MethodPost, "/runtime/workflow/start", http.StatusServiceUnavailable)
}

func TestReadinessChecksDependencies(t *testing.T) {
	dependencyErr := atomic.Bool{}
	health := &healthState{
		checks: []func(context.Context) error{
			func(context.Context) error {
				if dependencyErr.Load() {
					return errors.New("database unavailable")
				}
				return nil
			},
		},
	}
	health.ready.Store(true)
	handler := newHTTPHandler(http.NotFoundHandler(), health, nil, nil)

	assertStatus(t, handler, http.MethodGet, "/health/ready", http.StatusOK)
	dependencyErr.Store(true)
	assertStatus(t, handler, http.MethodGet, "/health/ready", http.StatusServiceUnavailable)
}

func TestHealthEndpointsRequireGET(t *testing.T) {
	handler := newHTTPHandler(http.NotFoundHandler(), &healthState{}, nil, nil)
	assertStatus(t, handler, http.MethodPost, "/health/live", http.StatusMethodNotAllowed)
	assertStatus(t, handler, http.MethodPost, "/health/ready", http.StatusMethodNotAllowed)
}

func assertStatus(t *testing.T, handler http.Handler, method, path string, want int) {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	if recorder.Code != want {
		t.Fatalf("%s %s status = %d, want %d; body=%s", method, path, recorder.Code, want, recorder.Body.String())
	}
}
