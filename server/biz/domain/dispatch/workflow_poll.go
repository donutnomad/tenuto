package dispatch

import (
	"github.com/donutnomad/tenuto/protocol"
)

type WorkflowTaskRun struct {
	Namespace      string
	WorkflowID     string
	WorkflowType   string
	ProgramVersion string
}

type WorkflowTaskWorker struct {
	Identity string
	BuildID  string
}

type WorkflowTaskState struct {
	ExecutionGeneration  int64
	StickyGeneration     int64
	StickyWorkerIdentity string
}

// PrepareWorkflowTask 按 Run 与领取的 Worker 补全 lease 的任务字段。
//
// sticky 投递的代数或目标 Worker 与 execution state 不一致时（sticky 会话已被清除或换了 Worker），
// 这份投递按普通投递交出：与 TaskQ 在 sticky 期限到期后的回落（RoutingFallback）相同。它是否仍属于 open
// 的 Workflow Task，由领取时的事务判定；在这里丢弃会让仍 open、只有这一份投递的 Workflow Task 永久丢失。
func PrepareWorkflowTask(task protocol.Task, run WorkflowTaskRun, worker WorkflowTaskWorker, state *WorkflowTaskState, executorABI string) protocol.Task {
	if task.Namespace == "" {
		task.Namespace = protocol.NormalizeNamespace(run.Namespace)
	}
	task.WorkflowType = run.WorkflowType
	task.ProgramVersion = run.ProgramVersion
	task.ExecutorABIVersion = executorABI
	// Program 版本是内容寻址的，同一 Workflow 类型加版本就唯一确定可执行内容。
	task.ArtifactFingerprint = run.WorkflowType + "@" + run.ProgramVersion
	task.WorkerIdentity = worker.Identity
	task.WorkerBuildID = worker.BuildID
	if state != nil {
		if task.WorkflowTaskRoute == protocol.WorkflowTaskRouteSticky &&
			(state.StickyGeneration != task.StickyGeneration || state.StickyWorkerIdentity != worker.Identity) {
			task.WorkflowTaskRoute = protocol.WorkflowTaskRouteNormal
			task.StickyTarget = ""
		}
		task.ExecutionGeneration = state.ExecutionGeneration
		if task.WorkflowTaskRoute != protocol.WorkflowTaskRouteSticky {
			task.StickyGeneration = state.StickyGeneration
		}
	}
	return task
}

func ValidateTaskNamespace(task protocol.Task, runNamespace string) error {
	want := protocol.NormalizeNamespace(runNamespace)
	got := protocol.NormalizeNamespace(task.Namespace)
	if got != want {
		return protocol.InvalidRequestf("task namespace %s does not match run namespace %s", got, want)
	}
	return nil
}
