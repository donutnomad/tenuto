package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/runtimeapi"
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

// 非长轮询请求超过在途上限时立即应答 503 与 Retry-After；长轮询不计入上限。
func TestLimitInFlightRejectsExcessRequests(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 4)
	handler := limitInFlight(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}), 2)
	serve := func(path string) chan int {
		done := make(chan int, 1)
		go func() {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))
			done <- recorder.Code
		}()
		return done
	}
	first, second := serve("/runtime/workflow/signal"), serve("/runtime/activity/complete")
	<-entered
	<-entered

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/runtime/workflow/start", nil))
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") == "" {
		t.Fatalf("third request = %d, Retry-After %q", recorder.Code, recorder.Header().Get("Retry-After"))
	}
	poll := serve("/runtime/activity/poll")
	<-entered // 长轮询越过上限进入 handler

	close(release)
	for _, done := range []chan int{first, second, poll} {
		if code := <-done; code != http.StatusOK {
			t.Fatalf("admitted request = %d", code)
		}
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/runtime/workflow/start", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("request after the slots were released = %d", recorder.Code)
	}
}

// Update 在拿锁与写库阶段占用在途名额（长等待之前由 engine.WithLongWaitRelease 归还）。
func TestLimitInFlightCountsUpdateUntilItWaits(t *testing.T) {
	waiting := make(chan struct{})
	finish := make(chan struct{})
	handler := limitInFlight(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == runtimeapi.WorkflowRuntimeUpdatePath {
			<-finish
		}
		w.WriteHeader(http.StatusOK)
	}), 1)
	done := make(chan int, 1)
	go func() {
		recorder := httptest.NewRecorder()
		close(waiting)
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, runtimeapi.WorkflowRuntimeUpdatePath, nil))
		done <- recorder.Code
	}()
	<-waiting
	deadline := time.Now().Add(3 * time.Second)
	for {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, runtimeapi.WorkflowRuntimeSignalPath, nil))
		if recorder.Code == http.StatusServiceUnavailable {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("update did not take an in-flight slot")
		}
	}
	close(finish)
	if code := <-done; code != http.StatusOK {
		t.Fatalf("update = %d", code)
	}
}
