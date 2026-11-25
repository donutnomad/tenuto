package executionview_test

import (
	"errors"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/execution/executiontest"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/view/executionview"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

// ============================================
// executionview 读模型测试
// ============================================
// 测试策略:
// - 执行域仓储用内存 ExecutionManager，定义与 Schedule 仓储用 infra/memory
// - 活跃 Worker 用本文件的 fake
// - 需要 Server 推进 Run 的场景（blocked 信息的产生）在 engine 的测试里
// ============================================

type fakeWorkers struct {
	workers []protocol.ActivityWorkerVersion
	err     error
}

func (w *fakeWorkers) ListActiveActivityWorkerVersions(time.Time) ([]protocol.ActivityWorkerVersion, error) {
	return w.workers, w.err
}

type testEnv struct {
	mgr       *memory.ExecutionManager
	configs   *memory.DefinitionRepository
	schedules *memory.ScheduleRepository
	workers   *fakeWorkers
	view      *executionview.View
}

func newEnv() *testEnv {
	env := &testEnv{
		mgr:       memory.NewExecutionManager(),
		configs:   memory.NewDefinitionRepository(),
		schedules: memory.NewScheduleRepository(),
		workers:   &fakeWorkers{},
	}
	env.view = executionview.New(env.mgr, env.configs, env.workers, env.schedules, systemclock.Clock{})
	return env
}

func (e *testEnv) createRun(t *testing.T, namespace, workflowID, runID string) {
	t.Helper()
	if err := e.mgr.RunInTx(func(tx execution.ExecutionTx) error {
		return tx.CreateRun(&execution.Run{
			ID: runID, Namespace: namespace, WorkflowID: workflowID, WorkflowType: workflowID, BranchToken: executiontest.BranchToken(runID),
			Status: history.StatusRunning, CreatedAt: time.Now(),
		})
	}); err != nil {
		t.Fatal(err)
	}
}

// createCurrentRun 建 Run 并把它设为该 workflow 的 current。
func (e *testEnv) createCurrentRun(t *testing.T, namespace, workflowID, runID string) {
	t.Helper()
	e.createRun(t, namespace, workflowID, runID)
	if err := e.mgr.RunInTx(func(tx execution.ExecutionTx) error {
		ok, err := tx.SetCurrentRunCAS(namespace, workflowID, "", runID)
		if err == nil && !ok {
			err = errors.New("current run already set")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func (e *testEnv) saveConfig(t *testing.T, namespace string, workflow *program.WorkflowConfig) {
	t.Helper()
	if _, err := e.configs.SaveWorkflowConfigInNamespace(namespace, workflow); err != nil {
		t.Fatal(err)
	}
}

func TestListNamespaces(t *testing.T) {
	// Happy Path: 有 Run 或活跃 Worker 的 namespace 各出一行，计数来自四个仓储，按名称排序
	env := newEnv()
	env.createRun(t, "tenant-a", "payments", "run-a1")
	env.createRun(t, "tenant-a", "payments", "run-a2")
	env.createRun(t, "", "orders", "run-default")
	env.saveConfig(t, "tenant-a", &program.WorkflowConfig{Type: "payments", Name: "v1", Program: programtest.Noop()})
	env.saveConfig(t, "tenant-a", &program.WorkflowConfig{Type: "payments", Name: "v2", Program: programtest.Noop()})
	env.saveConfig(t, "tenant-a", &program.WorkflowConfig{Type: "refunds", Program: programtest.Noop()})
	// w1 的两条路由（两个 task queue）算一个 Worker；只有 Worker 的 namespace 照样统计它的 Workflow。
	env.workers.workers = []protocol.ActivityWorkerVersion{
		{Namespace: "tenant-a", Identity: "w1", BuildID: "b1", TaskQueues: []string{"q1"}},
		{Namespace: "tenant-a", Identity: "w1", BuildID: "b1", TaskQueues: []string{"q2"}},
		{Namespace: "tenant-a", Identity: "w2"}, {Namespace: "workers-only", Identity: "w3"},
	}
	env.saveConfig(t, "workers-only", &program.WorkflowConfig{Type: "sync", Program: programtest.Noop()})
	if err := env.schedules.CreateSchedule(&schedule.Schedule{ID: "sch-1", Namespace: "tenant-a", WorkflowType: "payments"}); err != nil {
		t.Fatal(err)
	}

	result, err := env.view.ListNamespaces()
	if err != nil {
		t.Fatal(err)
	}
	want := []executionview.NamespaceInfo{
		{Name: "default", RunCount: 1},
		{Name: "tenant-a", RunCount: 2, WorkflowCount: 2, WorkerCount: 2, ScheduleCount: 1},
		{Name: "workers-only", WorkflowCount: 1, WorkerCount: 1},
	}
	if result.Total != len(want) || len(result.Items) != len(want) {
		t.Fatalf("result = %+v, want %+v", result, want)
	}
	for i := range want {
		if result.Items[i] != want[i] {
			t.Fatalf("item %d = %+v, want %+v", i, result.Items[i], want[i])
		}
	}
}

func TestListNamespacesWorkerError(t *testing.T) {
	// Error Case: 读取活跃 Worker 失败时返回该错误
	env := newEnv()
	env.workers.err = errors.New("worker store unavailable")
	if _, err := env.view.ListNamespaces(); !errors.Is(err, env.workers.err) {
		t.Fatalf("error = %v", err)
	}
}

// failingSchedules 让 Schedule 列表读取失败。
type failingSchedules struct {
	*memory.ScheduleRepository
	err error
}

func (s failingSchedules) ListSchedules(string) ([]*schedule.Schedule, error) { return nil, s.err }

func TestListNamespacesScheduleError(t *testing.T) {
	// Error Case: 读取 Schedule 失败时返回该错误，不把计数当作 0
	env := newEnv()
	env.createRun(t, "tenant-a", "payments", "run-a1")
	schedules := failingSchedules{ScheduleRepository: env.schedules, err: errors.New("schedule store unavailable")}
	view := executionview.New(env.mgr, env.configs, env.workers, schedules, systemclock.Clock{})
	if _, err := view.ListNamespaces(); !errors.Is(err, schedules.err) {
		t.Fatalf("error = %v", err)
	}
}

func TestListWorkflowConfigs(t *testing.T) {
	// Happy Path: 每个 workflow 一行，展示 active 版本的内容；目标 Worker 状态按 namespace 判定
	env := newEnv()
	env.saveConfig(t, "ns", &program.WorkflowConfig{Type: "b-online", Name: "B", TargetWorkerIdentity: "worker-b", Policy: "pinned", Version: "v7",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"a": {Symbol: "a", Type: "t"},
			"b": {Symbol: "b", Type: "t"},
		}})
	env.saveConfig(t, "ns", &program.WorkflowConfig{Type: "a-default", Name: "old", Program: programtest.Noop()})
	env.saveConfig(t, "ns", &program.WorkflowConfig{Type: "a-default", Name: "new", Program: programtest.Noop()})
	env.saveConfig(t, "ns", &program.WorkflowConfig{Type: "c-offline", TargetWorkerIdentity: "worker-c", Program: programtest.Noop()})
	if _, err := env.configs.SaveWorkflowConfigArtifactInNamespace("ns", &program.WorkflowConfig{Type: "d-artifact", Name: "artifact", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	env.saveConfig(t, "other", &program.WorkflowConfig{Type: "elsewhere", Program: programtest.Noop()})
	env.workers.workers = []protocol.ActivityWorkerVersion{
		{Namespace: "ns", Identity: "worker-b"},
		{Namespace: "other", Identity: "worker-c"}, // 其他 namespace 的同名 Worker 不算在线
	}

	result, err := env.view.ListWorkflowConfigs("ns")
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 4 || len(result.Items) != 4 {
		t.Fatalf("result = %+v", result)
	}
	byID := map[string]executionview.WorkflowConfigSummary{}
	for i, item := range result.Items {
		byID[item.WorkflowID] = item
		if i > 0 && result.Items[i-1].WorkflowID >= item.WorkflowID {
			t.Fatalf("items are not sorted by workflow id: %+v", result.Items)
		}
		if item.Namespace != "ns" {
			t.Fatalf("item namespace = %q", item.Namespace)
		}
	}
	if got := byID["a-default"]; got.Name != "new" || got.VersionCount != 2 || !got.Active || got.TargetWorkerStatus != "default" {
		t.Fatalf("a-default = %+v", got)
	}
	if got := byID["b-online"]; got.TargetWorkerIdentity != "worker-b" || got.TargetWorkerStatus != "online" ||
		got.ActivityCount != 2 || got.Policy != "pinned" || got.WorkflowVersion != "v7" || got.VersionCount != 1 {
		t.Fatalf("b-online = %+v", got)
	}
	if got := byID["c-offline"]; got.TargetWorkerStatus != "offline" {
		t.Fatalf("c-offline = %+v", got)
	}
	// Edge Case: 只登记为 artifact 的 workflow 展示已存版本，Active 为 false
	if got := byID["d-artifact"]; got.Active || got.Name != "artifact" || got.LatestVersion == "" {
		t.Fatalf("d-artifact = %+v", got)
	}

	// WorkflowConfigSummaries 是不带 Worker 状态的同一份汇总
	summaries, err := env.view.WorkflowConfigSummaries("ns")
	if err != nil {
		t.Fatal(err)
	}
	for _, summary := range summaries {
		if summary.TargetWorkerStatus != "" {
			t.Fatalf("summary carries worker status: %+v", summary)
		}
		want := byID[summary.WorkflowID]
		want.TargetWorkerStatus = ""
		if summary != want {
			t.Fatalf("summary = %+v, want %+v", summary, want)
		}
	}
}

func TestGetRunWithBlocked(t *testing.T) {
	t.Run("running run resolved by workflow id", func(t *testing.T) {
		// Happy Path: runID 为空时取 current Run；运行中的 Run 没有终态失败
		env := newEnv()
		env.createCurrentRun(t, "ns", "payments", "run-1")
		result, err := env.view.GetRunWithBlocked("ns", "payments", "")
		if err != nil {
			t.Fatal(err)
		}
		if result.Run.ID != "run-1" || result.Run.Status != history.StatusRunning || result.TerminalFailure != nil || result.Blocked.Blocked {
			t.Fatalf("result = %+v run=%+v", result, result.Run)
		}
	})
	t.Run("failed run carries the last terminal failure", func(t *testing.T) {
		// Happy Path: 失败 Run 的终态失败取自历史里的 WorkflowFailed 事件
		env := newEnv()
		env.createRun(t, "ns", "payments", "run-1")
		failed, err := history.NewWorkflowFailedFailureEvent(protocol.Failure{Message: "card declined", Type: "PaymentError", NonRetryable: true}, "")
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := execution.AppendEvent(env.mgr, "run-1", "run-1", "run-1", failed); err != nil {
			t.Fatal(err)
		}
		result, err := env.view.GetRunWithBlocked("ns", "", "run-1")
		if err != nil {
			t.Fatal(err)
		}
		if result.Run.Status != history.StatusFailed {
			t.Fatalf("status = %q, want failed", result.Run.Status)
		}
		if result.TerminalFailure == nil || result.TerminalFailure.Message != "card declined" ||
			result.TerminalFailure.Type != "PaymentError" || !result.TerminalFailure.NonRetryable {
			t.Fatalf("terminal failure = %+v", result.TerminalFailure)
		}
	})
	t.Run("missing identifiers", func(t *testing.T) {
		// Error Case: runID 与 workflowID 都为空
		env := newEnv()
		if _, err := env.view.GetRunWithBlocked("ns", "", ""); !errors.Is(err, execution.ErrRunOrWorkflowIDRequired) {
			t.Fatalf("error = %v, want ErrRunOrWorkflowIDRequired", err)
		}
	})
	t.Run("workflow without a current run", func(t *testing.T) {
		// Error Case: workflow 没有 current Run
		env := newEnv()
		if _, err := env.view.GetRunWithBlocked("ns", "never-started", ""); !errors.Is(err, execution.ErrNoCurrentRun) {
			t.Fatalf("error = %v, want ErrNoCurrentRun", err)
		}
	})
}

func TestGetEventsAndEventPage(t *testing.T) {
	env := newEnv()
	env.createCurrentRun(t, "ns", "payments", "run-1")
	const total = 5
	for i := 0; i < total; i++ {
		if _, err := env.mgr.Append("run-1", "run-1", "run-1", history.NewWorkflowSignaledEvent("tick", nil)); err != nil {
			t.Fatal(err)
		}
	}

	// Happy Path: GetEvents 返回完整历史，按事件 ID 递增
	events, err := env.view.GetEvents("ns", "payments", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != total {
		t.Fatalf("events = %d, want %d", len(events), total)
	}
	for i := 1; i < len(events); i++ {
		if events[i].ID <= events[i-1].ID {
			t.Fatalf("events are not ordered by id: %d then %d", events[i-1].ID, events[i].ID)
		}
	}

	// Happy Path: GetEventPage 按 workflowID 解析 current Run，逐页读完得到同样的事件
	var paged []history.Event
	request := execution.HistoryPageRequest{PageSize: 2}
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("pagination did not terminate")
		}
		page, err := env.view.GetEventPage("ns", "payments", request)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Events) > 2 {
			t.Fatalf("page holds %d events, want at most 2", len(page.Events))
		}
		paged = append(paged, page.Events...)
		if page.NextPageToken == "" {
			break
		}
		request.PageToken = page.NextPageToken
	}
	if len(paged) != total {
		t.Fatalf("paged events = %d, want %d", len(paged), total)
	}
	for i := range paged {
		if paged[i].ID != events[i].ID {
			t.Fatalf("paged event %d id = %d, want %d", i, paged[i].ID, events[i].ID)
		}
	}

	// Error Case: 两个标识都为空
	if _, err := env.view.GetEvents("ns", "", ""); !errors.Is(err, execution.ErrRunOrWorkflowIDRequired) {
		t.Fatalf("GetEvents error = %v", err)
	}
	if _, err := env.view.GetEventPage("ns", "", execution.HistoryPageRequest{}); !errors.Is(err, execution.ErrRunOrWorkflowIDRequired) {
		t.Fatalf("GetEventPage error = %v", err)
	}
}
