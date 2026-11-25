package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// chooseProgramFor 按 run 读取升级候选，在事务内为一个还没有 started 的 Workflow Task 选版；选版判定为配置错误时
// 随事务暂停 Run。
func chooseProgramFor(server *Server, run *execution.Run) (execution.ProgramVersionChoice, error) {
	return server.chooseWorkflowTaskProgram(context.Background(), run.ID, 0, server.upgradeCandidate(run))
}

// TestArtifactRegistrationKeepsLatest 固定发布协议：artifact 登记只新增内容版本，
// active/latest 保持原值；旧 Worker 重启不会把 latest 拉回旧版本。
func TestArtifactRegistrationKeepsLatest(t *testing.T) {
	server := newTestServer()
	refA, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{Type: "artifact-flow", Name: "A", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	refB, err := server.Definitions().RegisterWorkflowArtifactInNamespace("default", &program.WorkflowConfig{Type: "artifact-flow", Name: "B", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	if refA.Version == refB.Version {
		t.Fatal("artifact B must have a different content version")
	}
	active, err := server.activeWorkflowRefInNamespace("default", "artifact-flow")
	if err != nil {
		t.Fatal(err)
	}
	if active.Version != refA.Version {
		t.Fatalf("latest changed to %q after artifact registration, want %q", active.Version, refA.Version)
	}
	if _, err := server.workflowConfigs.GetWorkflowConfig(definition.WorkflowConfigRef{Namespace: "default", WorkflowID: "artifact-flow", Version: refB.Version}); err != nil {
		t.Fatalf("registered artifact version is not loadable: %v", err)
	}
}

// TestSetRunPolicyOverride 只改有效策略与来源，不动 current 内容。
func TestSetRunPolicyOverride(t *testing.T) {
	server := newTestServer()
	current := &execution.Run{ID: "policy-run", Namespace: "default", WorkflowID: "policy-flow", WorkflowType: "policy-flow", WorkflowConfigVersion: "v1", OriginalWorkflowConfigVersion: "v1", Policy: execution.RunPolicyAutoUpgrade, PolicySource: "definition", Status: history.StatusRunning, CreatedAt: time.Now()}
	if err := server.mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(current)) }); err != nil {
		t.Fatal(err)
	}
	updated, err := server.SetRunPolicy(context.Background(), "default", current.ID, "pinned")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Policy != execution.RunPolicyPinned || updated.PolicySource != execution.PolicySourceRunOverride {
		t.Fatalf("policy after override = %q/%q", updated.Policy, updated.PolicySource)
	}
	if updated.WorkflowConfigVersion != "v1" {
		t.Fatalf("override changed current content to %q", updated.WorkflowConfigVersion)
	}
	reloaded, err := server.mgr.Get(current.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Policy != execution.RunPolicyPinned {
		t.Fatalf("persisted policy = %q", reloaded.Policy)
	}
	if _, err := server.SetRunPolicy(context.Background(), "default", current.ID, "bogus"); err == nil {
		t.Fatal("unknown policy must be rejected")
	}
}

// TestAutoUpgradeRetrySelectsLatestAfterFailure 固定重试语义：最近一次失败不会锁定候选版本，
// 下一次 WFT 边界重新选择 latest。UpgradeError 只是诊断，不清空也不阻塞选版。
func TestAutoUpgradeRetrySelectsLatestAfterFailure(t *testing.T) {
	server := newTestServer()
	refA, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{Type: "retry-flow", Name: "A", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	refB, err := server.Definitions().RegisterWorkflowInNamespace("default", &program.WorkflowConfig{Type: "retry-flow", Name: "B", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	current := &execution.Run{
		ID: "retry-run", Namespace: "default", WorkflowID: "retry-flow", WorkflowType: "retry-flow",
		WorkflowConfigVersion: refA.Version, OriginalWorkflowConfigVersion: refA.Version,
		Policy: execution.RunPolicyAutoUpgrade, Status: history.StatusRunning, CreatedAt: time.Now(),
		UpgradeError: "auto upgrade to " + refB.Version + " failed; retrying with backoff",
	}
	if err := server.mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(current)) }); err != nil {
		t.Fatal(err)
	}
	started := history.NewWorkflowTaskStartedEvent(7, "worker", "build", 0)
	started.WorkflowTaskStarted.ProgramVersion = refB.Version
	if _, err := server.mgr.Append(current.ID, current.ID, current.ID, started); err != nil {
		t.Fatal(err)
	}
	failed := history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{ScheduledEventID: 7, Cause: "non_deterministic_error", Reason: "mismatch"})
	if _, err := server.mgr.Append(current.ID, current.ID, current.ID, failed); err != nil {
		t.Fatal(err)
	}

	selected, err := chooseProgramFor(server, current)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Version != refB.Version {
		t.Fatalf("retry selected %q, want latest %q", selected.Version, refB.Version)
	}
}

// TestUpgradeDiagnosticDoesNotBlockScheduling 固定 UpgradeError 的诊断语义：非空仍可调度
// 下一次 WFT；只有 ConfigError 才暂停推进。
func TestUpgradeDiagnosticDoesNotBlockScheduling(t *testing.T) {
	server := newTestServer()
	current := &execution.Run{
		ID: "upgrade-diagnostic-run", Namespace: "default", WorkflowID: "upgrade-diagnostic-flow", WorkflowType: "upgrade-diagnostic-flow",
		WorkflowConfigVersion: "vA", OriginalWorkflowConfigVersion: "vA",
		Policy: execution.RunPolicyAutoUpgrade, Status: history.StatusRunning, CreatedAt: time.Now(),
		UpgradeError: "auto upgrade to vB failed; retrying with backoff",
	}
	if err := server.mgr.RunInTx(func(tx execution.ExecutionTx) error { return tx.CreateRun(WithRootBranch(current)) }); err != nil {
		t.Fatal(err)
	}
	before := countEvents(t, server, current.ID)
	if err := server.scheduleWorkflowTaskIfRunning(testContext(), current.ID); err != nil {
		t.Fatal(err)
	}
	if after := countEvents(t, server, current.ID); after <= before {
		t.Fatalf("UpgradeError blocked scheduling: events %d -> %d", before, after)
	}
}
