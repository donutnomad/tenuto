package execution

import (
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
)

// NewContinuedRun starts a new History branch while preserving the pinned
// definition and execution deadline. ContinueAsNew resets the attempt.
func NewContinuedRun(old *Run, newID, namespace, workflowType, configVersion string, input map[string]any, now time.Time) *Run {
	next := NewRun(NewRunOptions{
		ID: newID, Namespace: namespace, WorkflowID: old.WorkflowID,
		WorkflowType: workflowType, WorkflowConfigVersion: configVersion,
		FirstRunID: old.FirstRunID, ContinuedRunID: old.ID, Input: input,
		SearchAttributes: old.SearchAttributes, DeadlineAt: old.DeadlineAt,
		Attempt: 1,
		// 子工作流 continue-as-new 后沿用父关联：接续链最后一个 run 的终态才传播给父 run。
		ParentRunID: old.ParentRunID, ParentChildID: old.ParentChildID,
		ParentScheduledEventID: old.ParentScheduledEventID,
		CreatedAt:              now,
	})
	inheritRunUpgradeState(next, old)
	return next
}

// CommitContinuedRun 在一个事务里建接续 Run next（经 CommitStartedRun，current 从旧 Run 切到它）并把旧 Run 置为
// ContinuedAsNew。WorkflowContinuedAsNew 事件先随 Workflow Task 提交，接续 Run 建好之前旧 Run 保持 Running，
// 恢复扫描据此补建接续 Run。旧 Run 在写入任何内容之前用 GetRunForUpdate 读取，next 的有效策略取旧 Run 此刻的值。
func CommitContinuedRun(tx ExecutionTx, oldRunID string, next *Run, treeID, branchID string, started history.Event) error {
	old, err := tx.GetRunForUpdate(oldRunID)
	if err != nil {
		return err
	}
	next.inheritPolicy(old)
	if err := CommitStartedRun(tx, next, oldRunID, treeID, branchID, started); err != nil {
		return err
	}
	return closeRun(tx, old, history.StatusContinuedAsNew, nil)
}

// inheritPolicy 让派生 Run 采用 old 的有效策略与来源，返回是否有改动。派生 Run 在事务之前按旧 Run 的读取结果构造，
// 提交它的事务用锁下读到的旧 Run 调用它，期间提交的 set-policy 因此不会丢失。
func (r *Run) inheritPolicy(old *Run) bool {
	if old.Policy == "" || r.Policy == old.Policy && r.PolicySource == old.PolicySource {
		return false
	}
	r.setPolicy(old.Policy)
	r.setPolicySource(old.PolicySource)
	return true
}

// inheritRunUpgradeState 让 CAN/retry/reset 派生 Run 继承内容策略与固定路由快照。
// original 保留整条 lineage 的启动来源；target 不因新内容版本或新配置而改变。
func inheritRunUpgradeState(next, old *Run) {
	next.OriginalWorkflowConfigVersion = old.OriginalWorkflowConfigVersion
	next.Policy = old.Policy
	next.PolicySource = old.PolicySource
	next.TargetWorkerIdentity = old.TargetWorkerIdentity
}
