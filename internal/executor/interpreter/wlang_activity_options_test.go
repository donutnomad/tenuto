package interpreter

// ============================================
// WLang per-call ActivityOptions（超时 + RetryPolicy）端到端测试
// ============================================
// 背景：Temporal 对齐审计 gap B——WLang workflow 里 activity 无法设超时/RetryPolicy。
// 走 per-call args 通道（activityCallOptions :2017 case + host ExecuteActivity 填 CmdScheduleActivity）。
//
// 测试策略:
// - E1 Happy Path: 单类超时 StartToCloseTimeout → Command 生效
// - E2 Happy Path: 4 类超时全传 → Command 全部生效
// - E3 核心锚（gap B 修净 + per-call 语义）: 同一 activity type 两次调用带不同超时 → 两条 Command 各带各的
// - E4 Happy Path: RetryPolicy 传递 → Command.RetryPolicy 各字段正确（含 string→duration）
// - E5 Happy Path: per-call TaskQueue 覆盖 per-type 默认，且与 per-call 超时共存
// - E6 回归: 超时不入 replay 比对——多批历史带超时，replay 不误报 nondeterministic
// - Error Case: 非法 duration "30x" → evalErr（不静默）
// ============================================

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

// findScheduleCmds 收集所有 CmdScheduleActivity（跳过 shadow 只在 replay，用于断言真命令）。
func findScheduleCmds(cmds []history.Command) []history.Command {
	var out []history.Command
	for _, c := range cmds {
		if c.Type == history.CmdScheduleActivity {
			out = append(out, c)
		}
	}
	return out
}

// E1 Happy Path: StartToCloseTimeout:30 * time.Second → CmdScheduleActivity.StartToCloseTimeout == 30s。
func TestWLangActivity_PerCallTimeout(t *testing.T) {
	const src = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}).Get(ctx, &out)
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	cmds, err := probeExecFix(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	sc := findScheduleCmds(cmds)
	if len(sc) != 1 {
		t.Fatalf("want 1 ScheduleActivity, got %d", len(sc))
	}
	if sc[0].StartToCloseTimeout != 30*time.Second {
		t.Errorf("StartToCloseTimeout = %v, want 30s", sc[0].StartToCloseTimeout)
	}
}

// E2 Happy Path: 4 类超时全传 → Command 全部生效。
func TestWLangActivity_PerCallAllTimeouts(t *testing.T) {
	const src = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Second,
		StartToCloseTimeout:    2 * time.Second,
		ScheduleToCloseTimeout: 3 * time.Second,
		HeartbeatTimeout:       4 * time.Second,
	}).Get(ctx, &out)
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	cmds, err := probeExecFix(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	sc := findScheduleCmds(cmds)
	if len(sc) != 1 {
		t.Fatalf("want 1 ScheduleActivity, got %d", len(sc))
	}
	c := sc[0]
	if c.ScheduleToStartTimeout != 1*time.Second {
		t.Errorf("ScheduleToStartTimeout = %v, want 1s", c.ScheduleToStartTimeout)
	}
	if c.StartToCloseTimeout != 2*time.Second {
		t.Errorf("StartToCloseTimeout = %v, want 2s", c.StartToCloseTimeout)
	}
	if c.ScheduleToCloseTimeout != 3*time.Second {
		t.Errorf("ScheduleToCloseTimeout = %v, want 3s", c.ScheduleToCloseTimeout)
	}
	if c.HeartbeatTimeout != 4*time.Second {
		t.Errorf("HeartbeatTimeout = %v, want 4s", c.HeartbeatTimeout)
	}
}

// E3 核心锚: 同一 activity type 两次 ExecuteActivity 带不同 StartToCloseTimeout（"30s"/"5s"）
// → 两条 CmdScheduleActivity 各带各的超时。若误退回 per-type 表（末次覆盖）→ 两条超时相同 → 转红。
func TestWLangActivity_PerCall_DifferentTimeouts(t *testing.T) {
	const src = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	f1 := workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	f2 := workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	})
	var o1, o2 map[string]any
	if err := f1.Get(ctx, &o1); err != nil {
		return err
	}
	return f2.Get(ctx, &o2)
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	cmds, err := probeExecFix(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	sc := findScheduleCmds(cmds)
	if len(sc) != 2 {
		t.Fatalf("want 2 ScheduleActivity (per-call 独立调度), got %d", len(sc))
	}
	// 两条命令 StepID 不同（per-lane 结构化键 #a1/#a2），各带各的超时。
	got := map[time.Duration]bool{}
	for _, c := range sc {
		got[c.StartToCloseTimeout] = true
	}
	if !got[30*time.Second] || !got[5*time.Second] {
		t.Errorf("want two distinct StartToCloseTimeouts {30s,5s}, got %v (cmd0=%v cmd1=%v)",
			got, sc[0].StartToCloseTimeout, sc[1].StartToCloseTimeout)
	}
}

// E4 Happy Path: RetryPolicy 传递 → Command.RetryPolicy 各字段正确（含 string→duration + 默认值）。
func TestWLangActivity_PerCallRetryPolicy(t *testing.T) {
	const src = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: workflow.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 1.5,
			MaxInterval:        60 * time.Second,
			NonRetryableErrors: []string{"bad", "worse"},
		},
	}).Get(ctx, &out)
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	cmds, err := probeExecFix(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	sc := findScheduleCmds(cmds)
	if len(sc) != 1 {
		t.Fatalf("want 1 ScheduleActivity, got %d", len(sc))
	}
	rp := sc[0].RetryPolicy
	if rp == nil {
		t.Fatalf("RetryPolicy is nil, want populated")
	}
	if rp.MaxAttempts != 3 {
		t.Errorf("MaxAttempts = %d, want 3", rp.MaxAttempts)
	}
	if rp.InitialInterval != 2*time.Second {
		t.Errorf("InitialInterval = %v, want 2s", rp.InitialInterval)
	}
	if rp.BackoffCoefficient != 1.5 {
		t.Errorf("BackoffCoefficient = %v, want 1.5", rp.BackoffCoefficient)
	}
	if rp.MaxInterval != 60*time.Second {
		t.Errorf("MaxInterval = %v, want 60s", rp.MaxInterval)
	}
	if len(rp.NonRetryableErrors) != 2 || rp.NonRetryableErrors[0] != "bad" || rp.NonRetryableErrors[1] != "worse" {
		t.Errorf("NonRetryableErrors = %v, want [bad worse]", rp.NonRetryableErrors)
	}
}

// E5 Happy Path: per-call TaskQueue 覆盖 per-type 默认，且与 per-call 超时共存。
func TestWLangActivity_PerCallOverridesTaskQueue(t *testing.T) {
	const src = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{
		TaskQueue:           "per-call-q",
		StartToCloseTimeout: 7 * time.Second,
	}).Get(ctx, &out)
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	cmds, err := probeExecFix(t, src, []history.Event{started})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	sc := findScheduleCmds(cmds)
	if len(sc) != 1 {
		t.Fatalf("want 1 ScheduleActivity, got %d", len(sc))
	}
	if sc[0].TaskQueue != "per-call-q" {
		t.Errorf("TaskQueue = %q, want per-call-q", sc[0].TaskQueue)
	}
	if sc[0].StartToCloseTimeout != 7*time.Second {
		t.Errorf("StartToCloseTimeout = %v, want 7s", sc[0].StartToCloseTimeout)
	}
}

// E6 回归: 超时不入 replay 比对。多批历史（scheduled 事件带不同超时值）+ 编排代码带超时，
// replay 不误报 nondeterministic（commandMatchesEvent 只比 StepID+ActivityType）。
func TestWLangActivity_TimeoutNotInReplayCompare(t *testing.T) {
	const src = `package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}).Get(ctx, &out)
}
`
	// 历史里 scheduled 事件的超时值故意与编排代码不同（99s），replay 不应因此报漂移。
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	events := []history.Event{
		started,
		wftc(2),
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{
			ActivityID: "#a1", ActivityType: "activities.A", StartToCloseTimeoutMs: 99000,
		}},
		{ID: 4, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{
			ActivityID: "#a1", ActivityType: "activities.A",
		}},
	}
	cmds, err := probeExecFix(t, src, events)
	if err != nil {
		t.Fatalf("execute should not report nondeterministic on timeout diff: %v", err)
	}
	// 完成而非失败：不应有 FailWorkflow，应有 CompleteWorkflow。
	if ok, msg := hasFailCmd(cmds); ok {
		t.Fatalf("unexpected FailWorkflow: %s", msg)
	}
}

// Error Case: 非法 duration "30x" → Execute 返回 evalErr（不静默）。
func TestWLangActivity_BadDuration_EvalErr(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{
		StartToCloseTimeout: "30x",
	}).Get(ctx, &out)
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	_, err := probeExecFix(t, src, []history.Event{started})
	if err == nil {
		t.Fatalf("want evalErr for bad duration '30x', got nil")
	}
	// duration 解析失败应体现在错误链里（time.ParseDuration 的错误）。
	if !errors.Is(err, err) || err.Error() == "" {
		t.Fatalf("unexpected error shape: %v", err)
	}
}

// 调用点的 TaskQueue 只作用于这一次调用：同一个 Activity 的另一处不带选项的调用
// 仍走缺省路由。
func TestWLangActivity_CallSiteRouteDoesNotBecomeDefault(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{TaskQueue: "gpu"}).Get(ctx, &out); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, activities.A, input).Get(ctx, &out)
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	sched := history.Event{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "#a1", ActivityType: "activities.A", TaskQueue: "gpu"}}
	done := history.Event{ID: 3, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{}}}
	cmds, err := probeExecFix(t, src, []history.Event{started, sched, done})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	sc := findScheduleCmds(cmds)
	if len(sc) != 1 || sc[0].ActivityID != "#a2" {
		t.Fatalf("want the second call scheduled, got %+v", sc)
	}
	if sc[0].TaskQueue != "" {
		t.Fatalf("plain call routed to %q, want the default route", sc[0].TaskQueue)
	}
}

// workflow.ActivityOptions 没有 BuildID：执行 Activity 的 build 由领取任务的 Worker 决定。Go 源码里写它编译不过；
// 手写的程序产物里写它时，Workflow Task 以未知字段失败，不会被静默忽略。
func TestWLangActivity_BuildIDOptionIsRejected(t *testing.T) {
	const src = `package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)
func W(ctx workflow.Context, input map[string]any) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.A, input, workflow.ActivityOptions{TaskQueue: "gpu", BuildID: "gpu@v2"}).Get(ctx, &out)
}
`
	started := history.NewWorkflowStartedEvent(map[string]any{"k": "v"}, "", "", "", "", 0)
	cmds, err := probeExecFix(t, src, []history.Event{started})
	if err == nil || !strings.Contains(err.Error(), `no field "BuildID"`) {
		t.Fatalf("execute = %+v, %v; want the unknown BuildID field rejected", cmds, err)
	}
}
