package definition

import programpkg "github.com/donutnomad/tenuto/program"

// ConfigRepository 持久化 workflow 配置内容版本与每个 workflow 的 active 引用。
// 消费方是 usecase/definitions、view/executionview 与执行核心。
type ConfigRepository interface {
	// SaveWorkflowConfigInNamespace 保存内容版本并把它设为 active。
	SaveWorkflowConfigInNamespace(namespace string, wf *programpkg.WorkflowConfig) (WorkflowConfigRef, error)
	// SaveWorkflowConfigArtifactInNamespace 保存内容版本，active 保持不变；旧 Worker 启动登记走这条路径。
	SaveWorkflowConfigArtifactInNamespace(namespace string, wf *programpkg.WorkflowConfig) (WorkflowConfigRef, error)
	// WorkflowConfigEnrollStateInNamespace 报告登记是否会产生激活动作：内容首次出现，
	// 或该 workflow 还没有 active 版本时返回 true。
	WorkflowConfigEnrollStateInNamespace(namespace string, ref WorkflowConfigRef) (bool, error)
	// EnrollWorkflowConfigInNamespace 登记内容版本，并在内容首次出现或该 workflow 还没有 active
	// 版本时激活它；activated 报告本次是否切换了 active。面向启动期自动登记：旧二进制重启
	// 送来已存在的 hash 时 active 保持不变。
	EnrollWorkflowConfigInNamespace(namespace string, wf *programpkg.WorkflowConfig) (ref WorkflowConfigRef, activated bool, err error)
	// GetWorkflowConfig 在版本不存在时返回满足 errors.Is(err, ErrWorkflowConfigNotFound) 的错误。
	// 内容版本不可变，返回的配置由全部调用方共享、只读；要改动时先复制（CloneWorkflowConfig，或只改顶层字段时
	// 复制结构体）。
	GetWorkflowConfig(ref WorkflowConfigRef) (*programpkg.WorkflowConfig, error)
	// LatestWorkflowConfigRefInNamespace 返回 active 引用；没有 active 时返回满足
	// errors.Is(err, ErrWorkflowConfigNotFound) 的错误。
	LatestWorkflowConfigRefInNamespace(namespace, workflowType string) (WorkflowConfigRef, error)
	// ActiveWorkflowConfigRefsInNamespace 返回 namespace 内每个 workflow 的 active 引用，按 Workflow Type 升序。
	ActiveWorkflowConfigRefsInNamespace(namespace string) ([]WorkflowConfigRef, error)
	// ListWorkflowConfigRefsInNamespace 返回 namespace 内全部已存内容版本，按 Workflow Type 升序，
	// 同一 workflow 内按首次保存的先后排列。
	ListWorkflowConfigRefsInNamespace(namespace string) ([]WorkflowConfigRef, error)
}
