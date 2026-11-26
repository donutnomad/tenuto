package execution

import (
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
)

// RunLookup 是解析请求所寻址 Run 需要的读取端口。
type RunLookup interface {
	Get(runID string) (*Run, error)
	GetCurrentRunID(namespace, workflowID string) (string, error)
}

// ResolveRun 加载请求按 (namespace, workflowID, runID) 寻址的 Run，所有按 Run ID 或 Workflow ID 寻址的操作都经它读取 Run。
//
// namespace 为空表示默认 namespace。给了 runID 时，Run 必须属于这个 namespace，给了 workflowID 时还必须属于这个
// Workflow；不属于时返回 ErrRunNotFound，与 Run 不存在时相同。只给 workflowID 时取这个 namespace 里它的 current Run。
// 两者都没给时返回 ErrRunOrWorkflowIDRequired。
func ResolveRun(runs RunLookup, namespace, workflowID, runID string) (*Run, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	if runID == "" {
		if workflowID == "" {
			return nil, ErrRunOrWorkflowIDRequired
		}
		current, err := runs.GetCurrentRunID(namespace, workflowID)
		if err != nil {
			return nil, err
		}
		runID = current
	}
	run, err := runs.Get(runID)
	if err != nil {
		return nil, err
	}
	if protocol.NormalizeNamespace(run.Namespace) != namespace || (workflowID != "" && run.WorkflowID != workflowID) {
		return nil, fmt.Errorf("run %s in namespace %s: %w", runID, namespace, ErrRunNotFound)
	}
	return run, nil
}

// RequireCurrentRunOf 确认按 (namespace, workflowID) 找到的 current Run 确实属于这个 Workflow，按字节比较。
// 存储的比较规则与 Go 不一致时（例如不区分大小写的排序规则），寻址会落到另一个 Workflow 的 Run 上；
// 这时返回 ErrCurrentRunConflict，不在别的 Workflow 上写入。
func RequireCurrentRunOf(run *Run, namespace, workflowID string) error {
	if protocol.NormalizeNamespace(run.Namespace) != protocol.NormalizeNamespace(namespace) || run.WorkflowID != workflowID {
		return fmt.Errorf("current run %s of workflow %q in namespace %q was found for workflow %q in namespace %q: %w",
			run.ID, run.WorkflowID, run.Namespace, workflowID, namespace, ErrCurrentRunConflict)
	}
	return nil
}
