package main

// replay_trace_run.go holds the per-lane bookkeeping buildTrace uses for
// pending frontiers and routine errors.

import (
	"sync"
)

// traceRun collects the pending frontier (if any) and error per lane.
type traceRun struct {
	mu sync.Mutex

	// pending maps laneID → the waiting activity that stalled it. First writer
	// wins (a lane stops at its first unresolved future).
	pending map[string]*waitingActivity

	// laneErr maps laneID → a real (non-pending) error message.
	laneErr map[string]string
}

func newTraceRun() *traceRun {
	return &traceRun{
		pending: map[string]*waitingActivity{},
		laneErr: map[string]string{},
	}
}

func (t *traceRun) recordFrontierForLane(lane string, wa waitingActivity) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.pending[lane]; !exists {
		w := wa
		t.pending[lane] = &w
	}
}

func (t *traceRun) pendingByLane() map[string]*waitingActivity {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]*waitingActivity, len(t.pending))
	for k, v := range t.pending {
		out[k] = v
	}
	return out
}

func (t *traceRun) errByLane() map[string]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]string, len(t.laneErr))
	for k, v := range t.laneErr {
		out[k] = v
	}
	return out
}
