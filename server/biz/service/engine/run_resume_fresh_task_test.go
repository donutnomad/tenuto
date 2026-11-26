package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// pauseBeforeC 构造一个以 ConfigError 暂停的 Run：Run 固定在声明 c 旧输入的内容 A 上，内容 B（声明 c 新输入，
// 与在线 Worker 的上报一致）已经是 latest。b 领取之后、完成之前暂停 Run，b 的完成不再推进，c 没有调度。
func pauseBeforeC(t *testing.T, name string) (*engine.Server, *memory.ExecutionManager, *memory.TaskQueue, string, definition.WorkflowConfigRef, definition.WorkflowConfigRef) {
	t.Helper()
	server, mgr, activities := newUpgradeReplayServer(t)
	fixture := newContractFixture(t, name, "v1", contractCInDefault, contractCOutInt)
	registerContractWorkerReporting(t, server, true, false)
	refA := fixture.register(t, server)
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", fixture.workflowID, fixture.workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.SetRunPolicy(context.Background(), protocol.DefaultNamespace, runID, execution.RunPolicyPinned); err != nil {
		t.Fatal(err)
	}
	writeContractActivities(t, fixture.root, contractCInWithNote, contractCOutInt, contractDInDefault)
	refB := fixture.register(t, server)
	if refB.Version == refA.Version {
		t.Fatal("B content version must differ from A")
	}

	b := dequeueActivityTaskWithin(t, activities, "example.b", 10*time.Second)
	if err := engine.PauseRunForTest(server, runID, "config error probe"); err != nil {
		t.Fatal(err)
	}
	if err := server.HandleActivityCompleted(context.Background(), b.RunID, b.ScheduledEventID, b.Attempt, b.Stamp, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := countScheduledType(mgrEvents(t, mgr, runID), "example.c"); got != 0 {
		t.Fatalf("paused run scheduled c %d times, want 0", got)
	}
	assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "paused run")
	return server, mgr, activities, runID, refA, refB
}

// 暂停的 Run 改成 auto_upgrade：恢复开启一个新的 Workflow Task，选版重新运行，Run 切到 latest 并完成。
func TestSetPolicyAutoUpgradeResumesPausedRunOnLatestProgram(t *testing.T) {
	server, mgr, activities, runID, refA, refB := pauseBeforeC(t, "resume-auto")

	if _, err := server.SetRunPolicy(context.Background(), protocol.DefaultNamespace, runID, execution.RunPolicyAutoUpgrade); err != nil {
		t.Fatal(err)
	}
	completeByTypeWithOutput(t, server, activities, "example.c", map[string]any{"amount": 42})
	completeByType(t, server, activities, "example.d")
	completeByType(t, server, activities, "example.e")
	waitForRunStatusWithin(t, server, runID, historypkg.StatusCompleted, 20*time.Second)

	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.ConfigError != "" || run.UpgradeError != "" {
		t.Fatalf("resumed run keeps errors: config=%q upgrade=%q", run.ConfigError, run.UpgradeError)
	}
	assertRunVersion(t, mgr, runID, refB.Version, refA.Version, "resumed run")
	events := mgrEvents(t, mgr, runID)
	if got := countScheduledType(events, "example.b"); got != 1 {
		t.Fatalf("b scheduled %d times, want 1 (completed result reused in replay)", got)
	}
}

// 暂停的 Run 改成 pinned：保持内容 A，c 按 A 的声明调度。在线 Worker 只上报新输入，调度不对照契约，
// Run 不再暂停；c 在队列里等兼容的 Worker，领取时对照。
func TestSetPolicyPinnedKeepsProgramAndSchedulesWithoutPausing(t *testing.T) {
	server, mgr, _, runID, refA, _ := pauseBeforeC(t, "resume-pinned")

	if _, err := server.SetRunPolicy(context.Background(), protocol.DefaultNamespace, runID, execution.RunPolicyPinned); err != nil {
		t.Fatal(err)
	}
	waitForScheduledType(t, mgr, runID, "example.c", 1)
	assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "pinned resume")
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.ConfigError != "" {
		t.Fatalf("a contract mismatch at schedule time paused the run again: %q", run.ConfigError)
	}
}
