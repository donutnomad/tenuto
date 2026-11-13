package dispatch

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

func TestPrepareWorkflowTaskStickyOwnerAndGeneration(t *testing.T) {
	run := WorkflowTaskRun{
		Namespace: "tenant-a", WorkflowID: "orders", WorkflowType: "orders", ProgramVersion: "p1",
	}
	worker := WorkflowTaskWorker{Identity: "worker-a", BuildID: "build-1"}
	state := &WorkflowTaskState{ExecutionGeneration: 1, StickyGeneration: 4, StickyWorkerIdentity: "worker-a"}
	sticky := protocol.Task{RunID: "run-1", WorkflowTaskRoute: protocol.WorkflowTaskRouteSticky, StickyTarget: "worker-a", StickyGeneration: 4}
	prepared := PrepareWorkflowTask(sticky, run, worker, state, "abi-1")
	if prepared.WorkflowTaskRoute != protocol.WorkflowTaskRouteSticky || prepared.Namespace != "tenant-a" ||
		prepared.WorkflowType != "orders" || prepared.ProgramVersion != "p1" ||
		prepared.ExecutorABIVersion != "abi-1" || prepared.WorkerIdentity != "worker-a" ||
		prepared.ExecutionGeneration != 1 || prepared.StickyGeneration != 4 {
		t.Fatalf("current sticky task prepared=%+v", prepared)
	}

	// sticky 会话已更替（代数旧了，或领取者不是会话的 Worker）：按普通投递交出，带上当前代数，
	// 由领取事务判定它是否仍属于 open 的 Workflow Task。
	stale := sticky
	stale.StickyGeneration = 3
	other := worker
	other.Identity = "worker-b"
	for name, tc := range map[string]struct {
		task   protocol.Task
		worker WorkflowTaskWorker
	}{
		"stale generation": {stale, worker},
		"other worker":     {sticky, other},
	} {
		prepared := PrepareWorkflowTask(tc.task, run, tc.worker, state, "abi-1")
		if prepared.WorkflowTaskRoute != protocol.WorkflowTaskRouteNormal || prepared.StickyTarget != "" ||
			prepared.StickyGeneration != 4 || prepared.ExecutionGeneration != 1 {
			t.Fatalf("%s: prepared=%+v", name, prepared)
		}
	}

	normal := protocol.Task{RunID: "run-1", WorkflowTaskRoute: protocol.WorkflowTaskRouteNormal}
	if prepared := PrepareWorkflowTask(normal, run, other, state, "abi-1"); prepared.StickyGeneration != 4 {
		t.Fatalf("normal task prepared=%+v", prepared)
	}
}

func TestValidateTaskNamespace(t *testing.T) {
	if err := ValidateTaskNamespace(protocol.Task{Namespace: ""}, protocol.DefaultNamespace); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTaskNamespace(protocol.Task{Namespace: "other"}, "tenant-a"); err == nil {
		t.Fatal("mismatched namespace was accepted")
	}
}
