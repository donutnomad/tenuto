package engine

import (
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/telemetry"
)

// TestDispatchActivityTaskStampsEnqueuedAt 断言**投递侧真的打了戳**。
//
// 前面的 telemetry 单测都是直接喂 Task 给 recordScheduleToStart，它们证明了记录逻辑对，
// 但证明不了生产路径会填这个字段。若 dispatchActivityTask 忘了打戳，那些单测照样全绿，
// 而线上指标恒为空——这正是"名字承诺了却从没验证"的那类恒过测试。本用例堵住这个缺口。
func TestDispatchActivityTaskStampsEnqueuedAt(t *testing.T) {
	mgr := memory.NewExecutionManager()
	queue := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), queue)

	before := time.Now()
	dispatched, err := server.dispatchActivityTask(t.Context(), protocol.Task{
		RunID:            "run-1",
		Namespace:        "default",
		ScheduledEventID: 5,
		ActivityType:     "send_email",
		TaskQueue:        "default",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !dispatched {
		t.Fatal("activity task was not dispatched")
	}
	after := time.Now()

	tasks := queue.snapshot()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 enqueued activity task, got %d", len(tasks))
	}
	stamped := tasks[0].Task.EnqueuedAt
	if stamped.IsZero() {
		t.Fatal("dispatchActivityTask did not stamp EnqueuedAt; " +
			"schedule-to-start would be permanently unrecordable on the activity path")
	}
	if stamped.Before(before) || stamped.After(after) {
		t.Fatalf("EnqueuedAt %v is outside the dispatch window [%v, %v]", stamped, before, after)
	}
}

// TestDispatchWorkflowTaskStampsEnqueuedAt 同上，覆盖 workflow 投递路径。
func TestDispatchWorkflowTaskStampsEnqueuedAt(t *testing.T) {
	mgr := memory.NewExecutionManager()
	queue := newTestTaskQueue()
	server := assembleServer(mgr, queue, newTestTaskQueue())

	before := time.Now()
	dispatched, err := server.dispatchWorkflowTask(t.Context(), protocol.Task{
		RunID:            "run-1",
		Namespace:        "default",
		ScheduledEventID: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !dispatched {
		t.Fatal("workflow task was not dispatched")
	}
	after := time.Now()

	tasks := queue.snapshot()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 enqueued workflow task, got %d", len(tasks))
	}
	stamped := tasks[0].Task.EnqueuedAt
	if stamped.IsZero() {
		t.Fatal("dispatchWorkflowTask did not stamp EnqueuedAt; " +
			"schedule-to-start would be permanently unrecordable on the workflow path")
	}
	if stamped.Before(before) || stamped.After(after) {
		t.Fatalf("EnqueuedAt %v is outside the dispatch window [%v, %v]", stamped, before, after)
	}
}

// TestRedispatchActivityTaskRestampsEnqueuedAt 钉住**重试重打戳**这条语义在真实路径上成立。
//
// 重派前先塞一个 10 分钟前的旧戳；若 dispatch 保留它，指标会把 backoff 等待
// 算成队列饥饿。断言新戳必须显著晚于旧戳。
func TestRedispatchActivityTaskRestampsEnqueuedAt(t *testing.T) {
	mgr := memory.NewExecutionManager()
	queue := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), queue)

	stale := time.Now().Add(-10 * time.Minute)
	task := protocol.Task{
		RunID:            "run-1",
		Namespace:        "default",
		ScheduledEventID: 5,
		ActivityType:     "send_email",
		TaskQueue:        "default",
		Attempt:          2,
		EnqueuedAt:       stale,
	}

	dispatched, err := server.redispatchActivityTask(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	if !dispatched {
		t.Fatal("activity task was not redispatched")
	}

	tasks := queue.snapshot()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 enqueued activity task, got %d", len(tasks))
	}
	stamped := tasks[0].Task.EnqueuedAt
	if !stamped.After(stale.Add(9 * time.Minute)) {
		t.Fatalf("redispatch kept the stale stamp %v (got %v); "+
			"retry backoff would be misreported as queue starvation", stale, stamped)
	}
}

// TestPollActivityTaskRecordsScheduleToStart 走完整条链路：
// 真投递（打戳）→ 真入队 → 真 poll（记录）→ 指标里出现样本。
//
// 这是唯一能证明"字段 + 打戳 + 记录"三者接得上的用例。
func TestPollActivityTaskRecordsScheduleToStart(t *testing.T) {
	providers, collect := newMetricProbe(t)
	mgr := memory.NewExecutionManager()
	queue := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), queue, WithTelemetry(providers))

	if _, err := server.dispatchActivityTask(t.Context(), protocol.Task{
		RunID:            "run-1",
		Namespace:        "default",
		ScheduledEventID: 5,
		ActivityType:     "send_email",
		TaskQueue:        "default",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := server.PollActivityTask(t.Context(), "default"); err != nil {
		t.Fatal(err)
	}

	metrics := collect()
	if got := histogramCount(t, metrics, scheduleToStartMetric, telemetry.AttrTaskType, string(protocol.TaskTypeActivity)); got != 1 {
		t.Fatalf("end-to-end dispatch→poll produced %d schedule-to-start sample(s), want 1", got)
	}
}

// TestPollActivityTaskSkipsLegacyTaskWithoutStamp 断言老消息在**真实 poll 路径**上
// 既不产生假样本，也不影响消费本身。
//
// 直接把一个没有 EnqueuedAt 的 task 塞进队列（模拟升级前入队的存量消息），
// 然后 poll：任务必须正常拿到，指标必须一条样本都没有。
func TestPollActivityTaskSkipsLegacyTaskWithoutStamp(t *testing.T) {
	providers, collect := newMetricProbe(t)
	mgr := memory.NewExecutionManager()
	queue := newTestTaskQueue()
	server := assembleServer(mgr, newTestTaskQueue(), queue, WithTelemetry(providers))

	// 绕过 dispatch 直接入队：这正是"存量老消息"在队列里的样子。
	if err := queue.Enqueue(t.Context(), protocol.Task{
		Type:             protocol.TaskTypeActivity,
		RunID:            "legacy-run",
		Namespace:        "default",
		TaskQueue:        "default",
		ScheduledEventID: 9,
		ActivityType:     "send_email",
	}); err != nil {
		t.Fatal(err)
	}

	lease, err := server.PollActivityTask(t.Context(), "default")
	if err != nil {
		t.Fatalf("legacy task without EnqueuedAt must still be consumable, got: %v", err)
	}
	if lease.Task.RunID != "legacy-run" {
		t.Fatalf("legacy task lost its identity: run id = %q", lease.Task.RunID)
	}

	if got := histogramCount(t, collect(), scheduleToStartMetric, "", ""); got != 0 {
		t.Fatalf("legacy task without a stamp produced %d sample(s), want 0", got)
	}
}
