package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ============================================
// list 页 blocked 可见性测试
// ============================================
// 测试策略（ListRuns 复用 computeBlockedInfo，吃已取的全量 events，零新增 IO）:
// - L1 Happy Path: blocked run 进 list，且与 detail 页 GetRunWithBlocked 逐字段相等（无漂移锚）
// - L2 Happy Path: 非 blocked（末个 WFT Completed，无 open）→ item.Blocked==nil
// - L3 Happy Path: BlockedOnly 过滤（5 run 2 blocked → 返回 2，Total==2）
// - L4 反例对照（🔴 核心变异锚点，内存尾切片，不依赖接口）: 早位置 open WFT + 尾部旧 WFT Failed
//     → computeBlockedInfo(全量)==false（正确）vs computeBlockedInfo(尾切片)==true（误判），二者相反
// - L5 Happy Path: open WFT 早位置 + fan-out ≥30 activity 命令事件 → Blocked==false（防把 fan-out 当失败段）
// - L6 Edge Case:  尾部 3 连 WFT Failed → ConsecutiveFailures==3；中夹 Completed → streak 从其后重计
// - L7 Edge Case:  终态 run（末事件 WorkflowCompleted/Failed）→ Blocked==false
// ============================================

// findListItem 在 ListRuns 结果里按 runID 找 item（未找到返回 nil）。
func findListItem(items []execution.RunListItem, runID string) *execution.RunListItem {
	for i := range items {
		if items[i].RunID == runID {
			return &items[i]
		}
	}
	return nil
}

// L1 Happy Path: blocked run 进 list；item.Blocked 与 detail 页 GetRunWithBlocked 逐字段相等。
func TestListRuns_BlockedRun_MatchesDetailBlockedInfo(t *testing.T) {
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-blocked", "wf-blocked", history.StatusRunning, true)
	seedBlockedHistory(t, mgr, "run-blocked", 2)

	result, err := svc.View().ListRuns(execution.RunListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	item := findListItem(result.Items, "run-blocked")
	if item == nil {
		t.Fatalf("expected run-blocked in list, got items %+v", result.Items)
	}
	if item.Blocked == nil || !item.Blocked.Blocked {
		t.Fatalf("expected item.Blocked.Blocked=true, got %+v", item.Blocked)
	}

	// 无漂移锚：与 detail 页 GetRunWithBlocked 对同一 run 逐字段相等。
	detail, err := svc.View().GetRunWithBlocked("", "", "run-blocked")
	if err != nil {
		t.Fatal(err)
	}
	if item.Blocked.Blocked != detail.Blocked.Blocked {
		t.Fatalf("Blocked mismatch: list=%v detail=%v", item.Blocked.Blocked, detail.Blocked.Blocked)
	}
	if item.Blocked.ConsecutiveFailures != detail.Blocked.ConsecutiveFailures {
		t.Fatalf("ConsecutiveFailures mismatch: list=%d detail=%d", item.Blocked.ConsecutiveFailures, detail.Blocked.ConsecutiveFailures)
	}
	if item.Blocked.Cause != detail.Blocked.Cause {
		t.Fatalf("Cause mismatch: list=%q detail=%q", item.Blocked.Cause, detail.Blocked.Cause)
	}
	if item.Blocked.Reason != detail.Blocked.Reason {
		t.Fatalf("Reason mismatch: list=%q detail=%q", item.Blocked.Reason, detail.Blocked.Reason)
	}
	if (item.Blocked.Since == nil) != (detail.Blocked.Since == nil) {
		t.Fatalf("Since nil-ness mismatch: list=%v detail=%v", item.Blocked.Since, detail.Blocked.Since)
	}
	if item.Blocked.Since != nil && !item.Blocked.Since.Equal(*detail.Blocked.Since) {
		t.Fatalf("Since mismatch: list=%v detail=%v", item.Blocked.Since, detail.Blocked.Since)
	}
}

// L2 Happy Path: 非 blocked run（末个 WFT Completed，无 open）→ item.Blocked==nil。
func TestListRuns_HealthyRun_BlockedNil(t *testing.T) {
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-healthy", "wf-healthy", history.StatusRunning, true)
	schedID, err := mgr.Append("run-healthy", "run-healthy", "run-healthy", history.NewWorkflowTaskScheduledEvent(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Append("run-healthy", "run-healthy", "run-healthy", history.NewWorkflowTaskCompletedEvent(schedID, "")); err != nil {
		t.Fatal(err)
	}

	result, err := svc.View().ListRuns(execution.RunListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	item := findListItem(result.Items, "run-healthy")
	if item == nil {
		t.Fatalf("expected run-healthy in list")
	}
	if item.Blocked != nil {
		t.Fatalf("expected item.Blocked==nil for healthy run, got %+v", item.Blocked)
	}
}

// L3 Happy Path: BlockedOnly 过滤——5 run（2 blocked / 3 healthy）→ 只返回 2，Total==2。
func TestListRuns_BlockedOnly_FiltersToBlocked(t *testing.T) {
	svc, mgr := newCurrentReadRuntime(t)
	// 2 个 blocked
	for _, id := range []string{"b1", "b2"} {
		seedRun(t, mgr, id, "wf-"+id, history.StatusRunning, true)
		seedBlockedHistory(t, mgr, id, 1)
	}
	// 3 个 healthy
	for _, id := range []string{"h1", "h2", "h3"} {
		seedRun(t, mgr, id, "wf-"+id, history.StatusRunning, true)
		schedID, err := mgr.Append(id, id, id, history.NewWorkflowTaskScheduledEvent(0))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := mgr.Append(id, id, id, history.NewWorkflowTaskCompletedEvent(schedID, "")); err != nil {
			t.Fatal(err)
		}
	}

	result, err := svc.View().ListRuns(execution.RunListFilter{BlockedOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 2 {
		t.Fatalf("expected 2 blocked items, got %d: %+v", len(result.Items), result.Items)
	}
	if result.Total != 2 {
		t.Fatalf("expected Total=2 (page-local count under BlockedOnly), got %d", result.Total)
	}
	for _, item := range result.Items {
		if item.Blocked == nil || !item.Blocked.Blocked {
			t.Fatalf("BlockedOnly item %s must carry Blocked, got %+v", item.RunID, item.Blocked)
		}
	}
}

// buildTailMisjudgeHistory 构造一段反例历史：早位置 open WFT（从未 close）被 fan-out 推离末尾，
// 尾部末端露出一个「已 close 的旧 WFT Failed」。
//   - 全量：早位置 Scheduled 从未 close → 有 open WFT → Blocked=false（正确）。
//   - 任意有界尾切片（漏看早位置 Scheduled、露出旧 Failed）：无 open + 末 WFT lifecycle 是 Failed → Blocked=true（误判）。
//
// events 的 ID 由内存构造显式赋值；返回值同时可喂给纯函数 computeBlockedInfo 与经 mgr 落库的 ListRuns。
func buildTailMisjudgeHistory(base time.Time) []history.Event {
	events := []history.Event{
		// 当前 open WFT：Scheduled(ID=1) 从未 close，位置被后续 fan-out 推离末尾
		wftScheduled(1, base),
		wftStarted(2, 1, base.Add(time.Second)),
	}
	// fan-out：一次 WFT 内产出大量 activity 命令事件，把 open Scheduled(ID=1) 推离末尾
	ts := base.Add(2 * time.Second)
	id := int64(3)
	for ; id <= 38; id++ { // ≥30 fan-out
		events = append(events, history.Event{ID: id, Timestamp: ts, ActivityScheduled: &history.ActivityScheduledEvent{ActivityType: "act"}})
		ts = ts.Add(time.Second)
	}
	// 尾部末端：一轮更旧的、已 close 的 WFT Failed（尾切片会命中它并误判）
	events = append(events,
		wftScheduled(id, ts),
		wftFailed(id+1, id, "non_deterministic_error", "old failure", ts.Add(time.Second)),
	)
	return events
}

// L4 反例对照（🔴 核心变异锚点）:
// 内存对照：全量 Blocked==false（有 open WFT）vs 尾切片 Blocked==true（误判），二者相反。
// ListRuns 落库对照：同一历史进 ListRuns → item.Blocked==nil。
// 变异锚点：若 ListRuns 的 computeBlockedInfo(events) 被改成吃尾切片（如 events[len-3:]），
// 该 run 尾窗漏看早位置 open WFT + 露出旧 Failed → 误判 Blocked → 本测转红。
func TestListRuns_BlockedMustReadFullHistory_TailSliceMisjudges(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	events := buildTailMisjudgeHistory(base)

	// 全量真值：有 open WFT（ID=1 未 close）→ Blocked=false。
	full := computeBlockedInfo(events)
	if full.Blocked {
		t.Fatalf("full-history computeBlockedInfo must be Blocked=false (open WFT present)")
	}
	// 尾切片误判：末 3 个事件漏看早位置 open Scheduled、露出旧 WFT Failed → 无 open + 末 lifecycle Failed → true。
	tail := events[len(events)-3:]
	tailInfo := computeBlockedInfo(tail)
	if !tailInfo.Blocked {
		t.Fatalf("tail-slice (missing early open WFT) must misjudge Blocked=true")
	}
	// 核心断言：全量与尾切片结论相反 → blocked 必须吃全量。
	if full.Blocked == tailInfo.Blocked {
		t.Fatalf("full and tail must disagree: full=%v tail=%v", full.Blocked, tailInfo.Blocked)
	}

	// 落库对照：同一历史经 ListRuns（全量判定）→ item.Blocked==nil。变异（吃尾切片）时此断言暴露误判。
	svc, mgr := newCurrentReadRuntime(t)
	seedRun(t, mgr, "run-fanout", "wf-fanout", history.StatusRunning, true)
	for _, e := range events {
		if _, err := mgr.Append("run-fanout", "run-fanout", "run-fanout", e); err != nil {
			t.Fatal(err)
		}
	}
	result, err := svc.View().ListRuns(execution.RunListFilter{})
	if err != nil {
		t.Fatal(err)
	}
	item := findListItem(result.Items, "run-fanout")
	if item == nil {
		t.Fatalf("expected run-fanout in list")
	}
	if item.Blocked != nil {
		t.Fatalf("run-fanout has an early open WFT → must not be blocked; tail-slice judgment would misjudge. got %+v", item.Blocked)
	}
}

// L5 Happy Path: open WFT 在早位置（Scheduled node 3）+ fan-out ≥30 activity 命令事件、尾部全 activity
// → 全量 Blocked==false（防把 fan-out 当失败段）。
func TestListRuns_OpenWFTEarly_FanOut_NotBlocked(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	events := []history.Event{
		wftScheduled(1, base),
		wftCompleted(2, 1, base.Add(time.Second)),
		wftScheduled(3, base.Add(2*time.Second)), // 当前 open WFT，从未 close
		wftStarted(4, 3, base.Add(3*time.Second)),
	}
	ts := base.Add(4 * time.Second)
	for i := int64(5); i <= 40; i++ { // ≥30 fan-out activity 命令事件
		events = append(events, history.Event{ID: i, Timestamp: ts, ActivityScheduled: &history.ActivityScheduledEvent{ActivityType: "act"}})
		ts = ts.Add(time.Second)
	}
	info := computeBlockedInfo(events)
	if info.Blocked {
		t.Fatalf("expected Blocked=false: open WFT early + fan-out tail must not be mistaken for a failed streak")
	}
}

// L6 Edge Case: 尾部 3 连 WFT Failed → ConsecutiveFailures==3；中夹 WFTCompleted → streak 从其后重计。
func TestListRuns_Streak_ThreeFailedThenBreak(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)

	// 尾部 3 连 Failed
	threeFailed := []history.Event{
		wftScheduled(1, base),
		wftFailed(2, 1, "non_deterministic_error", "e1", base.Add(time.Second)),
		wftScheduled(3, base.Add(2*time.Second)),
		wftFailed(4, 3, "non_deterministic_error", "e2", base.Add(3*time.Second)),
		wftScheduled(5, base.Add(4*time.Second)),
		wftFailed(6, 5, "non_deterministic_error", "e3", base.Add(5*time.Second)),
	}
	if info := computeBlockedInfo(threeFailed); !info.Blocked || info.ConsecutiveFailures != 3 {
		t.Fatalf("expected Blocked=true ConsecutiveFailures=3, got %+v", info)
	}

	// 中夹一个 Completed 后再 2 连 Failed → streak 从 Completed 之后重计为 2
	withBreak := []history.Event{
		wftScheduled(1, base),
		wftFailed(2, 1, "non_deterministic_error", "old1", base.Add(time.Second)),
		wftScheduled(3, base.Add(2*time.Second)),
		wftCompleted(4, 3, base.Add(3*time.Second)), // 断链
		wftScheduled(5, base.Add(4*time.Second)),
		wftFailed(6, 5, "non_deterministic_error", "e1", base.Add(5*time.Second)),
		wftScheduled(7, base.Add(6*time.Second)),
		wftFailed(8, 7, "non_deterministic_error", "e2", base.Add(7*time.Second)),
	}
	if info := computeBlockedInfo(withBreak); !info.Blocked || info.ConsecutiveFailures != 2 {
		t.Fatalf("expected streak recount to 2 after a Completed break, got %+v", info)
	}
}

// L7 Edge Case: 终态 run（末事件 WorkflowCompleted/Failed，最后一个 WFT 是 Completed）→ Blocked==false。
func TestListRuns_TerminalRun_NotBlocked(t *testing.T) {
	svc, mgr := newCurrentReadRuntime(t)

	// completed 终态
	seedRun(t, mgr, "run-done", "wf-done", history.StatusCompleted, true,
		history.NewWorkflowTaskScheduledEvent(0), history.NewWorkflowTaskCompletedEvent(1, ""), history.NewWorkflowCompletedEvent(nil))

	// failed 终态：最后一个 WFT 是 Completed（drive 出 CmdFailWorkflow），末事件 WorkflowFailed
	seedRun(t, mgr, "run-failed", "wf-failed", history.StatusFailed, true,
		history.NewWorkflowTaskScheduledEvent(0), history.NewWorkflowTaskCompletedEvent(1, ""), history.NewWorkflowFailedEvent("boom", ""))

	result, err := svc.View().ListRuns(execution.RunListFilter{IncludeTerminal: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"run-done", "run-failed"} {
		item := findListItem(result.Items, id)
		if item == nil {
			t.Fatalf("expected terminal run %s in list (IncludeTerminal)", id)
		}
		if item.Blocked != nil {
			t.Fatalf("expected terminal run %s Blocked==nil, got %+v", id, item.Blocked)
		}
	}
}
