package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

var InitiatorEnums = struct {
	Workflow string
	Retry    string
	Cron     string
}{Workflow: "", Retry: "Retry", Cron: "Cron"}

// Run
// @Setter
type Run struct {
	ID           string `json:"id"`
	Namespace    string `json:"namespace"`
	WorkflowID   string `json:"workflow_id"`
	WorkflowType string `json:"workflow_type,omitempty"`
	// WorkflowConfigVersion 是 Run 当前采用的内容版本，也就是 Program 版本：配置自带 Program，
	// 两者是同一个标识。它记录最近一次成功提交 WFT 所用的内容；AutoUpgrade 的候选版本
	// 只写在那一次 WFT 的 started 事件里，提交成功后才提升到这里。
	WorkflowConfigVersion string `json:"workflow_config_version"`
	// OriginalWorkflowConfigVersion 记录 Run 首次执行所选内容。
	OriginalWorkflowConfigVersion string `json:"original_workflow_config_version,omitempty"`
	// Policy 是 Run 的有效升级策略：RunPolicyAutoUpgrade 或 RunPolicyPinned，启动时写入。
	// PolicySource 记录来源：PolicySourceDefinition 或 PolicySourceRunOverride。
	Policy       string `json:"policy,omitempty"`
	PolicySource string `json:"policy_source,omitempty"`
	// TargetWorkerIdentity 是 Run 固定的 Activity worker 路由快照；内容升级不得改写它。
	TargetWorkerIdentity string `json:"target_worker_identity,omitempty"`
	// UpgradeError 记录最近一次 AutoUpgrade 尝试失败的诊断，供 Run/Console 查询。
	// 非空不阻塞推进：WFT 失败后按退避自动重试并在成功后清空；current/original 保持
	// 失败前的成功审计值，target 快照不变。
	UpgradeError string `json:"upgrade_error,omitempty"`
	// ConfigError 非空表示确定性配置/契约/target 冲突：任务不派发，暂停推进等待人工修复。
	ConfigError            string            `json:"config_error,omitempty"`
	ParentRunID            string            `json:"parent_run_id,omitempty"`
	ParentChildID          string            `json:"parent_child_id,omitempty"`
	ParentScheduledEventID int64             `json:"parent_scheduled_event_id,omitempty"`
	FirstRunID             string            `json:"first_run_id"`
	ContinuedRunID         string            `json:"continued_run_id,omitempty"`
	ResetRunID             string            `json:"reset_run_id,omitempty"`
	BranchToken            []byte            `json:"branch_token,omitempty"`
	Status                 history.Status    `json:"status"`
	Input                  map[string]any    `json:"input,omitempty"`
	Output                 map[string]any    `json:"output,omitempty"`
	SearchAttributes       map[string]string `json:"search_attributes,omitempty"`
	CreatedAt              time.Time         `json:"created_at"`
	DeadlineAt             *time.Time        `json:"deadline_at,omitempty"`
	RunDeadlineAt          *time.Time        `json:"run_deadline_at,omitempty"`
	Attempt                int               `json:"attempt,omitempty"`
	Initiator              string            `json:"initiator,omitempty"`
	IsCurrent              bool              `json:"is_current,omitempty"`
	patch                  RunPatch
}

const (
	// RunPolicyAutoUpgrade 让运行中的 Run 在后续 WFT 边界采用定义 latest 内容。
	RunPolicyAutoUpgrade = "auto_upgrade"
	// RunPolicyPinned 固定 Run 启动时的内容，只有显式迁移才会切换。
	RunPolicyPinned = "pinned"

	// PolicySourceDefinition 表示策略取自启动时的 Workflow 配置。
	PolicySourceDefinition = "definition"
	// PolicySourceRunOverride 表示策略由运行中的 set-policy 写入。
	PolicySourceRunOverride = "run_override"
)

// ValidateRunPolicy 校验 set-policy 传入的策略值。
func ValidateRunPolicy(policy string) error {
	switch policy {
	case RunPolicyAutoUpgrade, RunPolicyPinned:
		return nil
	case "":
		return protocol.InvalidRequestf("policy is required: supported values are %s and %s", RunPolicyAutoUpgrade, RunPolicyPinned)
	default:
		return protocol.InvalidRequestf("unknown workflow policy %q: supported values are %s and %s", policy, RunPolicyAutoUpgrade, RunPolicyPinned)
	}
}

// ApplyStartPolicy 在 Run 首次启动时写入策略、内容来源与固定路由快照。configPolicy 是 Workflow
// 配置声明的策略，没有声明时为 RunPolicyAutoUpgrade。派生 Run（continue-as-new、重试、reset）
// 已经继承了上一代的值，保持不变。
func (r *Run) ApplyStartPolicy(configPolicy, target string) error {
	if r.OriginalWorkflowConfigVersion == "" {
		r.OriginalWorkflowConfigVersion = r.WorkflowConfigVersion
	}
	if r.Policy == "" {
		if configPolicy == "" {
			configPolicy = RunPolicyAutoUpgrade
		}
		if err := ValidateRunPolicy(configPolicy); err != nil {
			return err
		}
		r.Policy = configPolicy
		r.PolicySource = PolicySourceDefinition
	}
	if r.TargetWorkerIdentity == "" {
		r.TargetWorkerIdentity = target
	}
	return nil
}
