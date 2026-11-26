package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// proposeWorkflowTaskProgram 为一个 Workflow Task 准备 Program 版本：按事务外读到的 run 读取升级候选，按
// execution.(*Run).ChooseProgramVersion 得出建议版本；run 已因 ConfigError 暂停时建议带上这条消息。建议只用来准备
// 派发（sticky 匹配、Worker 能力校验）；领取的事务按锁下读到的 Run 重新确定版本（execution.StartWorkflowTask），
// 两者不同时这次投递不开始。
//
// 选中的版本写入本次 WFT 的 task.ProgramVersion 与 started event，Run 的 current 字段保持不变；提交成功时才在
// CommitWorkflowTask 的事务里提升。每次 WFT 重试都重新读取 latest：重放失败只追加 WorkflowTaskFailed 事件并记录
// Run.UpgradeError 诊断，随后由统一的 WFT 失败退避机制重试。失败不会锁定候选版本，发布兼容内容后下一次正常重试即
// 自动接手。
func (s *Server) proposeWorkflowTaskProgram(run *execution.Run) (execution.UpgradeCandidate, execution.ProgramVersionChoice) {
	candidate := s.upgradeCandidate(run)
	choice := run.ChooseProgramVersion(candidate)
	if run.ConfigError != "" {
		choice.ConfigError = run.ConfigError
	}
	return candidate, choice
}

// chooseWorkflowTaskProgram 在一个事务里按锁下读到的 Run 确定 Workflow Task 的 Program 版本，选版判定为配置错误时
// 随这个事务暂停 Run，规则见 execution.ChooseWorkflowTaskProgram。与 WFT 完成、set-policy 共用 per-workflow keyed lock。
func (s *Server) chooseWorkflowTaskProgram(ctx context.Context, runID string, scheduledEventID int64, candidate execution.UpgradeCandidate) (execution.ProgramVersionChoice, error) {
	_, release, err := s.lockRunWorkflow(ctx, runID)
	if err != nil {
		return execution.ProgramVersionChoice{}, err
	}
	defer release()
	var choice execution.ProgramVersionChoice
	err = s.telemetry.runInTx(ctx, "choose_workflow_task_program", s.mgr, func(tx execution.ExecutionTx) error {
		var err error
		choice, err = execution.ChooseWorkflowTaskProgram(tx, runID, scheduledEventID, candidate)
		return err
	})
	return choice, err
}

// upgradeCandidate 读取 Run 的 Workflow Type 的 latest 版本；它是 Run 的升级候选时再读取候选内容，判定候选内容能否
// 读回 History 里已有的载荷（payloadConflict）；Run 固定了目标 Worker 时还读取活跃的 Activity Worker 与路由目录，
// 判定候选内容与目标 Worker 是否冲突。latest 读不到时没有候选；活跃 Worker、当前内容或 History 读不到时对应的判定
// 无法完成，记在 TargetCheckErr / PayloadCheckErr，本次不采用候选。
func (s *Server) upgradeCandidate(current *execution.Run) execution.UpgradeCandidate {
	if !current.AutoUpgrades() {
		return execution.UpgradeCandidate{}
	}
	latest, err := s.activeWorkflowRefInNamespace(current.Namespace, current.WorkflowType)
	if err != nil || !current.IsUpgradeCandidate(latest.Version) {
		return execution.UpgradeCandidate{}
	}
	candidate := execution.UpgradeCandidate{Version: latest.Version}
	workflow, err := s.workflowConfigs.GetWorkflowConfig(latest)
	if err != nil {
		candidate.ContentErr = err
		return candidate
	}
	candidate.PayloadConflict, candidate.PayloadCheckErr = s.payloadConflict(current, latest.Version, workflow)
	if current.TargetWorkerIdentity == "" {
		return candidate
	}
	active, err := s.workerVersions.ListActiveActivityWorkerVersions(s.clock.Now())
	if err != nil {
		candidate.TargetCheckErr = err
		return candidate
	}
	s.mu.Lock()
	catalog := s.activityRoutes
	s.mu.Unlock()
	candidate.TargetConflict = catalog.TargetWorkerConflict(current.Namespace, current.TargetWorkerIdentity, latest.Version, workflow, active)
	return candidate
}

// payloadConflict 判定 Run 换到 candidate 内容后，History 里已有的载荷能否被新 Program 读回原值（规则见
// program.ComparePayloadTypes）。两份内容之间的不兼容位置按版本对缓存；有不兼容位置时才读 History，只有 History
// 里出现过的位置算冲突。当前内容或 History 读不到时返回 checkErr。
func (s *Server) payloadConflict(run *execution.Run, version string, candidate *program.WorkflowConfig) (conflict, checkErr error) {
	key := payloadCompatKey{namespace: run.Namespace, workflowType: run.WorkflowType, current: run.WorkflowConfigVersion, candidate: version}
	result, ok := s.payloadCompat.get(key)
	if !ok {
		current, err := s.workflowConfigs.GetWorkflowConfig(runConfigRef(run, ""))
		if err != nil {
			return nil, err
		}
		result.incompatibilities, result.err = program.ComparePayloadTypes(current, candidate)
		s.payloadCompat.put(key, result)
	}
	if result.err != nil {
		return fmt.Errorf("auto upgrade to %s refused: cannot check payload compatibility: %w", version, result.err), nil
	}
	if len(result.incompatibilities) == 0 {
		return nil, nil
	}
	events, err := s.mgr.GetEvents(run.ID)
	if err != nil {
		return nil, err
	}
	return execution.PayloadConflictError(version, result.incompatibilities, events), nil
}

// payloadCompatKey 是一对内容版本；内容按版本不可变，结果可以一直复用。
type payloadCompatKey struct {
	namespace, workflowType, current, candidate string
}

type payloadCompatResult struct {
	incompatibilities []program.PayloadIncompatibility
	err               error
}

// payloadCompatCache 缓存 payloadCompatKey 的对照结果，超过 payloadCompatCacheSize 时整体清空：一次发布只产生
// 少数几对版本，清空后按需重算。
type payloadCompatCache struct {
	mu      sync.Mutex
	results map[payloadCompatKey]payloadCompatResult
}

const payloadCompatCacheSize = 1024

func (c *payloadCompatCache) get(key payloadCompatKey) (payloadCompatResult, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	result, ok := c.results[key]
	return result, ok
}

func (c *payloadCompatCache) put(key payloadCompatKey, result payloadCompatResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.results == nil || len(c.results) >= payloadCompatCacheSize {
		c.results = make(map[payloadCompatKey]payloadCompatResult)
	}
	c.results[key] = result
}

// SetRunPolicy 是运行中 override 入口：只改 Run 的有效策略与来源，不改 current 内容。
// 切到 auto_upgrade 后由下一个 WFT 边界重新选版；切到 pinned 立即固定当前内容。
// 因 ConfigError 暂停的 Run 由它恢复：恢复从一个新的 WFT 开始（见 execution.OverrideRunPolicy），
// auto_upgrade 因此在恢复时就切到 latest，候选内容照常要通过对 History 的重放；pinned 保持当前内容。
// 返回事务内锁下读到并写入策略后的 Run。
func (s *Server) SetRunPolicy(ctx context.Context, namespace, runID, policy string) (*execution.Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := execution.ValidateRunPolicy(policy); err != nil {
		return nil, err
	}
	ref, err := s.resolveRunRef(namespace, "", runID)
	if err != nil {
		return nil, err
	}
	release, err := s.keyedMu.Lock(context.Background(), workflowLockKey(ref.Namespace, ref.WorkflowID))
	if err != nil {
		return nil, err
	}
	defer release()
	// 策略与恢复时关闭的 Workflow Task 在同一事务提交。
	// 请求的 ctx 只用于入口的取消检查；写入与调度不随请求取消。
	writeCtx := context.Background()
	var updated *execution.Run
	if err := s.telemetry.runInTx(writeCtx, "set_run_policy", s.mgr, func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate(ref.ID)
		if err != nil {
			return err
		}
		treeID, branchID, err := runBranchInfoFromRun(run)
		if err != nil {
			return err
		}
		updated = run
		return execution.OverrideRunPolicy(tx, run, policy, treeID, branchID, s.telemetry.inject(writeCtx))
	}); err != nil {
		return nil, err
	}
	// 显式恢复入口：暂停中的 Run 在策略更新后重新调度 WFT；未暂停的 Run 无影响。
	if err := s.scheduleWorkflowTaskIfRunning(writeCtx, ref.ID); err != nil {
		return nil, err
	}
	return updated, nil
}

// pauseRun 在事务内锁下读取 Run，把 ConfigError 写入并暂停它。调用方持有 workflow 的 keyed lock。
func (s *Server) pauseRun(runID, message string) error {
	return s.telemetry.runInTx(context.Background(), "pause_run", s.mgr, func(tx execution.ExecutionTx) error {
		run, err := tx.GetRunForUpdate(runID)
		if err != nil {
			return err
		}
		return execution.PauseRun(tx, run, message)
	})
}
