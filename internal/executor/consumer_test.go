package executor

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// levelRecorder 记录每条日志的级别与消息。
type levelRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *levelRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *levelRecorder) Handle(_ context.Context, record slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, record)
	return nil
}
func (r *levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *levelRecorder) WithGroup(string) slog.Handler      { return r }

// 连续的轮询失败按指数退避等待，只有第一次记 WARN，其余记 DEBUG，恢复时记一次 INFO；不记 ERROR。
func TestConsumeBacksOffAndLogsOncePerOutage(t *testing.T) {
	recorder := &levelRecorder{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pollTimes []time.Time
	poll := func(context.Context) (protocol.TaskLease, error) {
		pollTimes = append(pollTimes, time.Now())
		if len(pollTimes) <= 4 {
			return protocol.TaskLease{}, errors.New("connection refused")
		}
		return protocol.TaskLease{Token: "lease"}, nil
	}
	handled := 0
	Consume(ctx, ctx, slog.New(recorder), "workflow task", poll, func(context.Context, protocol.TaskLease) {
		handled++
		cancel()
	})
	if handled != 1 || len(pollTimes) != 5 {
		t.Fatalf("handled %d leases after %d polls, want 1 after 5", handled, len(pollTimes))
	}
	gaps := make([]time.Duration, 0, 4)
	for i := 1; i < len(pollTimes); i++ {
		gaps = append(gaps, pollTimes[i].Sub(pollTimes[i-1]))
	}
	if gaps[0] < 70*time.Millisecond || gaps[3] < 2*gaps[0] {
		t.Fatalf("gaps between polls = %v, want exponential backoff from 100ms", gaps)
	}
	levels := map[slog.Level]int{}
	for _, record := range recorder.records {
		levels[record.Level]++
	}
	if levels[slog.LevelError] != 0 || levels[slog.LevelWarn] != 1 || levels[slog.LevelDebug] != 3 || levels[slog.LevelInfo] != 1 {
		t.Fatalf("log levels = %v, want 1 WARN, 3 DEBUG, 1 INFO and no ERROR", levels)
	}
}
