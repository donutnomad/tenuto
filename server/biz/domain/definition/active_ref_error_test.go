package definition_test

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/server/biz/domain/definition"
)

// failingConfigs 的 active 查询返回固定错误，其余方法不参与本测试。
type failingConfigs struct {
	definition.ConfigRepository
	err error
}

func (c failingConfigs) LatestWorkflowConfigRefInNamespace(string, string) (definition.WorkflowConfigRef, error) {
	return definition.WorkflowConfigRef{}, c.err
}

func TestActiveWorkflowRefKeepsRepositoryErrors(t *testing.T) {
	// Error Case: 仓储故障原样透出，调用方能和"未登记"区分
	storeDown := errors.New("connection refused")
	_, err := definition.ActiveWorkflowRef(failingConfigs{err: storeDown}, "ns", "payments")
	if !errors.Is(err, storeDown) {
		t.Fatalf("error = %v, want it to wrap the repository error", err)
	}
	if errors.Is(err, definition.ErrWorkflowConfigNotFound) {
		t.Fatalf("a repository failure must not read as not-found: %v", err)
	}

	// Error Case: 没有 active 版本时保留 ErrWorkflowConfigNotFound
	_, err = definition.ActiveWorkflowRef(failingConfigs{err: definition.ErrWorkflowConfigNotFound}, "ns", "payments")
	if !errors.Is(err, definition.ErrWorkflowConfigNotFound) {
		t.Fatalf("error = %v, want ErrWorkflowConfigNotFound", err)
	}
}
