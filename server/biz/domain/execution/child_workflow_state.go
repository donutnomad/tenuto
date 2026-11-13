package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

type ChildWorkflowStatus string

// 字符串枚举 + var 结构体形式（项目规范）。child 无 generation/reset/重试语义（决策 2，纯值无 stamp）：
// Initiated（父已发起、尚未建子 Run）→ Started（子 Run 已建、运行中）→ 4 个终态之一。
// 终态由子→父传播（propagateChildWorkflowClose）写入，与子 Run 自身的终态对应。
var ChildWorkflowStatusEnums = struct {
	Initiated  ChildWorkflowStatus
	Started    ChildWorkflowStatus
	Completed  ChildWorkflowStatus
	Failed     ChildWorkflowStatus
	Cancelled  ChildWorkflowStatus
	Terminated ChildWorkflowStatus
}{
	Initiated:  "initiated",
	Started:    "started",
	Completed:  "completed",
	Failed:     "failed",
	Cancelled:  "cancelled",
	Terminated: "terminated",
}

// ChildWorkflowState 是父 workflow 视角下一个子 workflow 关联（以父 ScheduledEventID =
// 父 ChildWorkflowStarted 事件 ID 为生命周期 key）的"当前状态"唯一权威来源。判断"子是否已 close /
// 是否仍 open / 是否可被 recovery 重建"一律读这里，不扫父 history。
//
// RunID = 父 RunID（不是子 RunID）；ChildRunID = 实际子 Run（initiated 阶段父侧已预生成但
// 语义上"子 Run 尚未建"，started 阶段确认）。与 ActivityState/TimerState 不同：child 无
// Attempt/Stamp（决策 2：无重试/reset/generation，同主键只一代 child）；幂等由 Status 终态
// 在 RunInTx 内的 GetChildWorkflowStateForUpdate 校验保证。
// ChildWorkflowState
// @Setter
type ChildWorkflowState struct {
	RunID            string `json:"runId"`            // 复合主键之一 = 父 RunID
	ScheduledEventID int64  `json:"scheduledEventId"` // 复合主键之二 = 父 ChildWorkflowStarted 事件 ID，贯穿生命周期不变
	ChildID          string `json:"childId"`          // 父侧稳定 ID（replay/去重 key，executor 用）
	ChildRunID       string `json:"childRunId"`       // 实际子 Run ID（started 后非空）

	WorkflowID            string `json:"workflowId"`            // 子 workflow 配置 ID
	WorkflowConfigVersion string `json:"workflowConfigVersion"` // 子配置版本（recovery 重建子 Run 用）

	Status ChildWorkflowStatus `json:"status"`
	// ParentClosePolicy 取自父 Run 的 ChildWorkflowStarted 事件，父 Run 关闭时按它处理子 Workflow。
	ParentClosePolicy protocol.ParentClosePolicy `json:"parentClosePolicy,omitempty"`
	// CancelRequested 表示父 Workflow 已请求取消这个子 Workflow（ChildWorkflowCancelRequested）。
	// 子 Run 的取消请求在提交后发出，进程中途退出时由子 Run 的恢复补发。
	CancelRequested bool `json:"cancelRequested,omitempty"`

	Input  map[string]any `json:"input,omitempty"`  // 启动子 Run / recovery 重建用
	Output map[string]any `json:"output,omitempty"` // completed 终态结果
	Error  string         `json:"error,omitempty"`  // failed 终态原因
	Reason string         `json:"reason,omitempty"` // terminated 终态原因

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	patch     ChildWorkflowStatePatch
}

func (s *ChildWorkflowState) IsStarted() bool { return s.Status == ChildWorkflowStatusEnums.Started }

// IsTerminal 报告子 workflow 是否已终结（completed/failed/cancelled/terminated）。
// 子→父传播写终态前在 RunInTx 内校验非终态（幂等，替代 childWorkflowAlreadyClosed 历史扫描）；
// cancel 级联 / recovery / reset guard 用 !IsTerminal() 求"仍 open 的 child"。
func (s *ChildWorkflowState) IsTerminal() bool {
	switch s.Status {
	case ChildWorkflowStatusEnums.Completed, ChildWorkflowStatusEnums.Failed,
		ChildWorkflowStatusEnums.Cancelled, ChildWorkflowStatusEnums.Terminated:
		return true
	}
	return false
}

// ChildWorkflowStateStore 是 child_workflow_states 表的 CRUD。返回的 *ChildWorkflowState
// 必须是深拷贝（实现侧负责），调用方修改不得影响存储内部。方法签名与 ActivityStateStore /
// TimerStateStore 同构（复合主键 RunID+ScheduledEventID）。
type ChildWorkflowStateStore interface {
	GetChildWorkflowState(runID string, scheduledEventID int64) (*ChildWorkflowState, bool, error)
	UpsertChildWorkflowState(state *ChildWorkflowState) error
	ListChildWorkflowStates(runID string) ([]*ChildWorkflowState, error)
}
