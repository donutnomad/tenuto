package engine_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// pauseOnOldContract 复现「旧 build 下线」：Run 固定在声明 c@v1 的内容 A 上，路由目录里只有
// 上报 c@v2 的新 Worker，内容 B（声明 c@v2）已经是 latest。b 完成后调度 c 时契约不一致，Run 暂停。
func pauseOnOldContract(t *testing.T, name string) (*engine.Server, *memory.ExecutionManager, *memory.TaskQueue, string, definition.WorkflowConfigRef, definition.WorkflowConfigRef) {
	t.Helper()
	server, mgr, activities := newUpgradeReplayServer(t)
	fixture := newContractFixture(t, name, "v1", contractCInDefault, contractCOutInt)
	registerContractWorkerVersions(t, server, "v2", "v1")
	refA := fixture.register(t, server)
	runID, err := startedRunID(server.StartWorkflow(context.Background(), "", fixture.workflowID, fixture.workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.SetRunPolicy(context.Background(), protocol.DefaultNamespace, runID, execution.RunPolicyPinned); err != nil {
		t.Fatal(err)
	}
	writeContractActivities(t, fixture.root, "v2", "v1", contractCInDefault, contractCOutInt)
	refB := fixture.register(t, server)
	if refB.Version == refA.Version {
		t.Fatal("B content version must differ from A")
	}

	completeByType(t, server, activities, "example.b")
	waitForRunConfigErrorForTest(t, mgr, runID)
	paused, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(paused.ConfigError, `version expected "v1" actual "v2"`) {
		t.Fatalf("ConfigError = %q, want the c version mismatch", paused.ConfigError)
	}
	assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "paused run")
	return server, mgr, activities, runID, refA, refB
}

// 暂停的 Run 改成 auto_upgrade：恢复开启一个新的 Workflow Task，选版重新运行，Run 切到 latest 并完成。
func TestSetPolicyAutoUpgradeResumesPausedRunOnLatestProgram(t *testing.T) {
	server, mgr, activities, runID, refA, refB := pauseOnOldContract(t, "resume-auto")

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
	// 暂停时打开的 Workflow Task 由恢复关闭，History 里留下一条记录。
	resumed := 0
	for _, event := range events {
		if event.WorkflowTaskFailed != nil && event.WorkflowTaskFailed.Cause == execution.WorkflowTaskFailedCauseRunResumed {
			resumed++
		}
	}
	if resumed != 1 {
		t.Fatalf("WorkflowTaskFailed(cause=%s) = %d, want 1", execution.WorkflowTaskFailedCauseRunResumed, resumed)
	}
}

// 暂停的 Run 改成 pinned：保持当前内容；不一致仍在，Run 再次暂停。
func TestSetPolicyPinnedKeepsProgramAndPausesAgain(t *testing.T) {
	server, mgr, _, runID, refA, _ := pauseOnOldContract(t, "resume-pinned")

	if _, err := server.SetRunPolicy(context.Background(), protocol.DefaultNamespace, runID, execution.RunPolicyPinned); err != nil {
		t.Fatal(err)
	}
	waitForRunConfigErrorForTest(t, mgr, runID)
	assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "pinned resume")
	if got := countScheduledType(mgrEvents(t, mgr, runID), "example.c"); got != 0 {
		t.Fatalf("c scheduled %d times against a mismatched worker, want 0", got)
	}
}
