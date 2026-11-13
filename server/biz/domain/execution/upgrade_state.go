package execution

import (
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
)

// RecordUpgradeFailure 在 Workflow Task 失败时记录 AutoUpgrade 诊断，供 Run 与 Console 查询，返回 Run 是否有改动。
// candidate 是该 WFT started 时选中的 Program 版本；它与 Run 的 current 版本相同时失败与升级无关，Run 不变。
// 该字段不阻塞推进：WFT 失败后由统一退避机制重试，current/original 保持最后成功值。
// 调用方把它与关闭那次 WFT 的 WorkflowTaskFailed 写在同一事务里。
func (r *Run) RecordUpgradeFailure(candidate string, scheduledEventID int64, cause, reason string) bool {
	if candidate == "" || candidate == r.WorkflowConfigVersion {
		return false
	}
	message := fmt.Sprintf("auto upgrade to %s failed at workflow task %d: %s: %s; retrying with backoff", candidate, scheduledEventID, cause, reason)
	if r.UpgradeError == message {
		return false
	}
	r.setUpgradeError(message)
	return true
}

// MarkConfigError 记录确定性配置/契约/target 冲突并暂停该 Run，返回 Run 是否有改动。
func (r *Run) MarkConfigError(message string) bool {
	if r.ConfigError == message {
		return false
	}
	r.setConfigError(message)
	return true
}

// PauseRun 把确定性配置/契约/target 冲突写到 Run 上：任务不再派发，人工修复后由 set-policy 恢复。
// run 是本事务用 GetRunForUpdate 读到的 Run。
func PauseRun(tx ExecutionTx, run *Run, message string) error {
	if !run.MarkConfigError(message) {
		return nil
	}
	return tx.UpdateRun(run)
}

// OverridePolicy 写入运行中策略 override，并清除升级诊断与暂停状态；current 内容不变。
func (r *Run) OverridePolicy(policy string) error {
	if err := ValidateRunPolicy(policy); err != nil {
		return err
	}
	r.setPolicy(policy)
	r.setPolicySource(PolicySourceRunOverride)
	r.setUpgradeError("")
	r.setConfigError("")
	return nil
}

// OverrideRunPolicy 写入运行中策略 override，清除升级诊断与暂停状态，current 内容不变。run 是本事务用
// GetRunForUpdate 读到的 Run；它已结束时返回 InvalidRequest，事务不做修改。Run 因 ConfigError 暂停时，
// 暂停期间已被 Worker 领取的 Workflow Task 在同一事务里以 run_resumed 关闭：它的 WorkflowTaskStarted 记录着暂停前
// 选中的 Program，重新投递同一个任务时 Server 仍按那一版的声明对照契约；调用方随后调度的新 WFT 在新的任务边界按新策略
// 选版。还没有被领取的任务没有选版记录，保持打开。
func OverrideRunPolicy(tx ExecutionTx, run *Run, policy, treeID, branchID string, traceContext map[string]string) error {
	state, err := tx.GetWorkflowExecutionStateForUpdate(run.ID)
	if err != nil {
		return err
	}
	if state.Status != history.StatusRunning {
		return protocol.InvalidRequestf("cannot set policy: workflow %s is %s", run.ID, state.Status)
	}
	paused := run.ConfigError != ""
	if err := run.OverridePolicy(policy); err != nil {
		return err
	}
	if err := tx.UpdateRun(run); err != nil || !paused {
		return err
	}
	open := state.OpenWorkflowTaskEventID
	if open == 0 || state.WorkflowTask.StartedScheduledEventID != open {
		return nil
	}
	_, _, err = tx.AppendWorkflowTaskClosed(run.ID, treeID, branchID, history.NewWorkflowTaskFailedEvent(history.WorkflowTaskFailedEvent{
		ScheduledEventID: open,
		Cause:            WorkflowTaskFailedCauseRunResumed,
		Reason:           "run resumed by set-policy; the next workflow task selects the program again",
		TraceContext:     cloneStringMap(traceContext),
	}))
	return err
}

// adoptProgramVersion 在 Workflow Task 成功提交时把 current 提升为本次采用的版本并清除上一次
// AutoUpgrade 尝试的诊断，返回 Run 是否有改动。original 与 target 快照保持不变。
func (r *Run) adoptProgramVersion(version string) bool {
	changed := false
	if version != "" && r.WorkflowConfigVersion != version {
		r.setWorkflowConfigVersion(version)
		changed = true
	}
	if r.UpgradeError != "" {
		r.setUpgradeError("")
		changed = true
	}
	return changed
}

// AutoUpgrades 报告 Run 是否在 Workflow Task 边界采用 latest Program：运行中且策略是 auto_upgrade。
func (r *Run) AutoUpgrades() bool {
	return r.Status == history.StatusRunning && r.Policy == RunPolicyAutoUpgrade
}

// IsUpgradeCandidate 报告 version 是否是 Run 在下一个 Workflow Task 边界要尝试采用的版本：
// Run 按 AutoUpgrades 采用 latest，且 version 非空、不同于当前版本。
func (r *Run) IsUpgradeCandidate(version string) bool {
	return r.AutoUpgrades() && version != "" && version != r.WorkflowConfigVersion
}

// UpgradeCandidate 是 Workflow Type 的 latest Program 版本，以及调用方为它读到的事实。
type UpgradeCandidate struct {
	// Version 是 latest 版本；没有可用的 latest 时为空。
	Version string
	// ContentErr 是读取候选内容的错误。
	ContentErr error
	// TargetConflict 是候选内容与 Run 固定的目标 Worker 之间的冲突。
	TargetConflict error
	// TargetCheckErr 表示目标 Worker 冲突无法判定（活跃 Worker 读不到）。
	TargetCheckErr error
	// PayloadConflict 是 History 里已有的载荷在候选内容的类型下读不回原值（见 PayloadConflictError）。
	PayloadConflict error
	// PayloadCheckErr 表示载荷兼容无法判定（当前内容或 History 读不到）。
	PayloadCheckErr error
}

// ProgramVersionChoice 是一个 Workflow Task 采用的 Program 版本。
type ProgramVersionChoice struct {
	Version string
	// ConfigError 非空时 Run 以这条消息暂停，本次 Workflow Task 不派发。
	ConfigError string
}

// ChooseProgramVersion 是 AutoUpgrade 的选版规则，在每个 Workflow Task 边界执行。
//
// 候选版本不是 IsUpgradeCandidate 时沿用当前版本。候选内容读不到、与 Run 固定的目标 Worker 冲突，或读不回 History 里
// 已有的载荷，是确定性配置错误：沿用当前版本，并以 ConfigError 暂停 Run，等待人工处理，不自动修改 target。目标 Worker
// 冲突或载荷兼容无法判定时本次沿用当前版本，不暂停，下一个 Workflow Task 再判定。其余情况采用候选版本；选中的版本只写入本次 Workflow Task，Workflow Task
// 提交成功时才提升为 Run 的当前版本。
func (r *Run) ChooseProgramVersion(candidate UpgradeCandidate) ProgramVersionChoice {
	current := ProgramVersionChoice{Version: r.WorkflowConfigVersion}
	if !r.IsUpgradeCandidate(candidate.Version) {
		return current
	}
	if candidate.ContentErr != nil {
		current.ConfigError = fmt.Sprintf("candidate workflow content %s is unavailable: %v", candidate.Version, candidate.ContentErr)
		return current
	}
	if candidate.TargetConflict != nil {
		current.ConfigError = candidate.TargetConflict.Error()
		return current
	}
	if candidate.PayloadConflict != nil {
		current.ConfigError = candidate.PayloadConflict.Error()
		return current
	}
	if candidate.TargetCheckErr != nil || candidate.PayloadCheckErr != nil {
		return current
	}
	return ProgramVersionChoice{Version: candidate.Version}
}

// ChooseWorkflowTaskProgram 在事务 tx 里为 scheduledEventID 这个 Workflow Task 确定 Program 版本，依据本事务用
// GetRunForUpdate 读到的 Run 与加锁读到的 execution state：
//   - Run 已因 ConfigError 暂停时返回带这条消息的 ConfigError，任务不派发；
//   - 这个 WFT 已经 started 过时沿用 WorkflowTaskStarted 记录的版本；
//   - 否则按 ChooseProgramVersion 对 candidate 选版；选版判定为配置错误时在本事务里以 PauseRun 暂停 Run。
//
// candidate 由调用方在事务之前读取；Run 在那之后改成 auto_upgrade 时 candidate 为空，本次沿用当前版本。
func ChooseWorkflowTaskProgram(tx ExecutionTx, runID string, scheduledEventID int64, candidate UpgradeCandidate) (ProgramVersionChoice, error) {
	run, err := tx.GetRunForUpdate(runID)
	if err != nil {
		return ProgramVersionChoice{}, err
	}
	if run.ConfigError != "" {
		return ProgramVersionChoice{Version: run.WorkflowConfigVersion, ConfigError: run.ConfigError}, nil
	}
	state, err := tx.GetWorkflowExecutionStateForUpdate(runID)
	if err != nil {
		return ProgramVersionChoice{}, err
	}
	if version := state.WorkflowTaskProgramVersion(scheduledEventID); version != "" {
		return ProgramVersionChoice{Version: version}, nil
	}
	choice := run.ChooseProgramVersion(candidate)
	if choice.ConfigError != "" {
		if err := PauseRun(tx, run, choice.ConfigError); err != nil {
			return ProgramVersionChoice{}, err
		}
	}
	return choice, nil
}
