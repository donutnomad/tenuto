package executor

import (
	"errors"

	"github.com/donutnomad/tenuto/protocol"
)

// ExecutionCacheKeyForTask 用 Server 写在 task 上的 Workflow 类型与 artifact fingerprint 组成 cache key。
func ExecutionCacheKeyForTask(task protocol.Task, executorABI string) (ExecutionCacheKey, error) {
	if task.WorkflowType == "" {
		return ExecutionCacheKey{}, errors.New("workflow task type is empty")
	}
	if task.ArtifactFingerprint == "" {
		return ExecutionCacheKey{}, errors.New("workflow task artifact fingerprint is empty")
	}
	return ExecutionCacheKey{
		Namespace: protocol.NormalizeNamespace(task.Namespace), RunID: task.RunID,
		WorkflowType: task.WorkflowType, ArtifactFingerprint: task.ArtifactFingerprint,
		ExecutorABIVersion: executorABI,
	}, nil
}
