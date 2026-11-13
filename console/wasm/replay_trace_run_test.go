package main

import "testing"

// TestTraceRun_FirstFrontierWins
// Edge Case: 同一 lane 多次记录前沿,只保留第一个(lane 停在首个未决 future);不同 lane 互不影响。
func TestTraceRun_FirstFrontierWins(t *testing.T) {
	tr := newTraceRun()
	tr.recordFrontierForLane("main", waitingActivity{ActivityID: "first", Type: "greet", Kind: "activity"})
	tr.recordFrontierForLane("main", waitingActivity{ActivityID: "second", Type: "other", Kind: "activity"})
	tr.recordFrontierForLane("routine-1", waitingActivity{ActivityID: "lane-act", Type: "t", Kind: "activity"})

	got := tr.pendingByLane()
	if got["main"] == nil || got["main"].ActivityID != "first" {
		t.Fatalf("expected first frontier to win, got %+v", got["main"])
	}
	if got["routine-1"] == nil || got["routine-1"].ActivityID != "lane-act" {
		t.Fatalf("routine-1 frontier = %+v", got["routine-1"])
	}
}
