package executionrepo

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"gorm.io/gorm"
)

// executionStateProjectionJSON 是 blocked_json 列的内容：BlockedInfo 的字段平铺在顶层，
// 其余是 execution state 里没有独立列的投影字段。
type executionStateProjectionJSON struct {
	execution.BlockedInfo
	WorkflowTask       execution.WorkflowTaskProjection `json:"workflow_task"`
	HasWorkflowStarted bool                             `json:"has_workflow_started,omitempty"`
	CancelRequested    bool                             `json:"cancel_requested,omitempty"`
}

func executionStateToRow(state *execution.WorkflowExecutionState) (*WorkflowExecutionStatePO, error) {
	searchAttributes, err := json.Marshal(state.SearchAttributes)
	if err != nil {
		return nil, err
	}
	currentWaiting, err := json.Marshal(state.CurrentWaiting)
	if err != nil {
		return nil, err
	}
	openWaits, err := json.Marshal(state.OpenWaits)
	if err != nil {
		return nil, err
	}
	blocked, err := json.Marshal(executionStateProjectionJSON{
		BlockedInfo: state.Blocked, WorkflowTask: state.WorkflowTask,
		HasWorkflowStarted: state.HasWorkflowStarted, CancelRequested: state.CancelRequested,
	})
	if err != nil {
		return nil, err
	}
	row := new(WorkflowExecutionStatePO).ToPO(state)
	row.SearchAttributesJSON = searchAttributes
	row.CurrentWaitingJSON = currentWaiting
	row.OpenWaitsJSON = openWaits
	row.BlockedJSON = blocked
	row.NeedsWorkflowTask = state.NeedsWorkflowTask()
	return row, nil
}

func executionStateRowToState(row *WorkflowExecutionStatePO) (*execution.WorkflowExecutionState, error) {
	state := &execution.WorkflowExecutionState{
		RunID: row.RunID, NextEventID: row.NextEventID, LastEventID: row.LastEventID,
		HistoryEventCount: row.HistoryEventCount, HistorySizeBytes: row.HistorySizeBytes,
		Status: history.Status(row.Status), ClosedAt: row.ClosedAt, FailureReason: row.FailureReason, IsReset: row.IsReset,
		OpenWorkflowTaskEventID: row.OpenWorkflowTaskEventID, ExecutionGeneration: row.ExecutionGeneration,
		CurrentActivationID: row.CurrentActivationID, CurrentActivationAttempt: row.CurrentActivationAttempt,
		StickyWorkerIdentity: row.StickyWorkerIdentity, StickyWorkerBuildID: row.StickyWorkerBuildID,
		StickyGeneration: row.StickyGeneration, StickyExpiresAt: row.StickyExpiresAt, StateVersion: row.StateVersion,
		UpdatedAt: row.UpdatedAt,
	}
	if len(row.SearchAttributesJSON) > 0 {
		if err := json.Unmarshal(row.SearchAttributesJSON, &state.SearchAttributes); err != nil {
			return nil, err
		}
	}
	if len(row.CurrentWaitingJSON) > 0 {
		if err := json.Unmarshal(row.CurrentWaitingJSON, &state.CurrentWaiting); err != nil {
			return nil, err
		}
	}
	if len(row.OpenWaitsJSON) > 0 {
		if err := json.Unmarshal(row.OpenWaitsJSON, &state.OpenWaits); err != nil {
			return nil, err
		}
	}
	if state.OpenWaits == nil {
		state.OpenWaits = make(map[int64]execution.WaitingState)
	}
	if len(row.BlockedJSON) > 0 {
		var projection executionStateProjectionJSON
		if err := json.Unmarshal(row.BlockedJSON, &projection); err != nil {
			return nil, err
		}
		state.Blocked = projection.BlockedInfo
		state.WorkflowTask = projection.WorkflowTask
		state.HasWorkflowStarted = projection.HasWorkflowStarted
		state.CancelRequested = projection.CancelRequested
	}
	return state, nil
}

// GetWorkflowExecutionState 返回 Run 的 execution state。状态行与 Run 行在同一事务内写入、一起删除，
// 状态行不存在时返回 ErrRunNotFound。
// 本方法走非事务连接，RunInTx 闭包内读取状态使用事务句柄。
func (s *Repository) GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error) {
	rows, err := infradb.Find(s.db, gsql.From[WorkflowExecutionStatePO](WorkflowExecutionStateSchema).
		Where(WorkflowExecutionStateSchema.RunID.Eq(runID)))
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("workflow execution state %s: %w", runID, execution.ErrRunNotFound)
	}
	return executionStateRowToState(rows[0])
}

// ListWorkflowExecutionStates 返回 runIDs 中仍存在的 Run 的 execution state。
func (s *Repository) ListWorkflowExecutionStates(runIDs []string) (map[string]*execution.WorkflowExecutionState, error) {
	if len(runIDs) == 0 {
		return map[string]*execution.WorkflowExecutionState{}, nil
	}
	rows, err := infradb.Find(s.db, gsql.From[WorkflowExecutionStatePO](WorkflowExecutionStateSchema).
		Where(WorkflowExecutionStateSchema.RunID.In(runIDs...)))
	if err != nil {
		return nil, err
	}
	result := make(map[string]*execution.WorkflowExecutionState, len(rows))
	for _, row := range rows {
		state, err := executionStateRowToState(row)
		if err != nil {
			return nil, err
		}
		result[state.RunID] = state
	}
	return result, nil
}

// ListExpiredStickyExecutions 实现 execution.WorkflowExecutionStateStore 的 sticky 回落扫描。
//
// 一条带 WHERE + LIMIT 的查询直接命中需要 sticky 回落的行，替代原来的
// "ListAll() 全表扫（无 WHERE 无 LIMIT，扫所有历史 run）+ 逐个 GetWorkflowExecutionState"
// 的 1+N 次查询。
//
// WHERE 三项与 engine 侧的 Go 过滤条件逐条取反等价：
//
//	StickyWorkerIdentity != ""   →  sticky_worker_identity <> ''
//	OpenWorkflowTaskEventID != 0 →  open_workflow_task_event_id > 0
//	StickyExpiresAt != nil && !StickyValidAt(now)
//	                             →  sticky_expires_at <= ?（NULL 不满足比较）
//	                                （StickyExpiresAt 是 open WFT 的 sticky schedule-to-start 期限，started 后清空）
//
// 索引 idx_wf_exec_states_sticky_fallback (sticky_worker_identity, sticky_expires_at)：
// 前缀列绝大多数行是 ”（无 sticky），选择性良好。
func (s *Repository) ListExpiredStickyExecutions(now time.Time, limit int) ([]*execution.WorkflowExecutionState, error) {
	query := gsql.From[WorkflowExecutionStatePO](WorkflowExecutionStateSchema).
		Where(
			WorkflowExecutionStateSchema.StickyWorkerIdentity.Not(""),
			WorkflowExecutionStateSchema.OpenWorkflowTaskEventID.Gt(0),
			WorkflowExecutionStateSchema.StickyExpiresAt.Lte(&now),
		).
		// 稳定顺序：避免多轮扫描在 LIMIT 截断处反复取到同一批之外的随机子集。
		Order(WorkflowExecutionStateSchema.RunID)
	if limit > 0 {
		query = query.Limit(limit)
	}
	rows, err := infradb.Find(s.db, query)
	if err != nil {
		return nil, err
	}
	states := make([]*execution.WorkflowExecutionState, 0, len(rows))
	for _, row := range rows {
		state, err := executionStateRowToState(row)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
}

func (s *Repository) AssignStickyExecution(runID string, executionGeneration, expectedStickyGeneration int64, workerIdentity, workerBuildID string) (generation int64, assigned bool, err error) {
	err = s.inTx(func(tx *gormExecutionTx) error {
		state, stateErr := tx.lockedState(runID)
		if stateErr != nil {
			return stateErr
		}
		assigned = state.AssignSticky(executionGeneration, expectedStickyGeneration, workerIdentity, workerBuildID, s.clock.Now())
		generation = state.StickyGeneration
		return nil
	})
	return generation, assigned, err
}

func (s *Repository) ClearStickyExecution(runID string, executionGeneration, expectedStickyGeneration int64) (generation int64, cleared bool, err error) {
	err = s.inTx(func(tx *gormExecutionTx) error {
		state, stateErr := tx.lockedState(runID)
		if stateErr != nil {
			return stateErr
		}
		cleared = state.ClearSticky(executionGeneration, expectedStickyGeneration, s.clock.Now())
		generation = state.StickyGeneration
		return nil
	})
	return generation, cleared, err
}

// createExecutionState 写入新 Run 的 execution state 行。
func createExecutionState(db *gorm.DB, state *execution.WorkflowExecutionState) error {
	row, err := executionStateToRow(state)
	if err != nil {
		return err
	}
	if err := db.Create(row).Error; err != nil {
		return err
	}
	state.ClearPatch()
	return nil
}

// updateExecutionState 把 state 经 setter 改过的字段写回，没有改动时不写库。
// blocked_json 由四个投影字段合成，其中任何一个改过就重写这一列；needs_workflow_task 随 WFT 状态与 open WFT 重算。
func updateExecutionState(db *gorm.DB, state *execution.WorkflowExecutionState) error {
	row, err := executionStateToRow(state)
	if err != nil {
		return err
	}
	changed := state.ExportPatch()
	patch := new(WorkflowExecutionStatePO).ToPatch(state)
	if changed.SearchAttributes.IsPresent() {
		patch["search_attributes_json"] = row.SearchAttributesJSON
	}
	if changed.CurrentWaiting.IsPresent() {
		patch["current_waiting_json"] = row.CurrentWaitingJSON
	}
	if changed.OpenWaits.IsPresent() {
		patch["open_waits_json"] = row.OpenWaitsJSON
	}
	if changed.Blocked.IsPresent() || changed.WorkflowTask.IsPresent() || changed.HasWorkflowStarted.IsPresent() || changed.CancelRequested.IsPresent() {
		patch["blocked_json"] = row.BlockedJSON
	}
	if changed.WorkflowTask.IsPresent() || changed.OpenWorkflowTaskEventID.IsPresent() {
		patch["needs_workflow_task"] = row.NeedsWorkflowTask
	}
	if len(patch) == 0 {
		return nil
	}
	if err := gsql.From[WorkflowExecutionStatePO](WorkflowExecutionStateSchema).
		Where(WorkflowExecutionStateSchema.RunID.Eq(state.RunID)).
		Update(db, patch).Error; err != nil {
		return err
	}
	state.ClearPatch()
	return nil
}

// lockedState 返回本事务持有行锁的 execution state。每个 Run 在一个事务里只加锁读一次：之后的追加与更新都改这一份，
// 事务提交前由 flushStates 一次写回改过的列。状态行不存在说明 Run 已被 retention GC 删除或从未创建，返回
// ErrRunNotFound，迟到的回调因此不会写出孤儿行。
//
// 写回推迟到提交前不改变其它事务看到的结果：行锁从第一次读取一直持有到提交，提交之前的写入本来就只有本事务可见。
// 本事务里读这一行一律经过这里，不直接查询表。
func (t *gormExecutionTx) lockedState(runID string) (*execution.WorkflowExecutionState, error) {
	if state, ok := t.states[runID]; ok {
		return state, nil
	}
	rows, err := infradb.Find(t.db, gsql.From[WorkflowExecutionStatePO](WorkflowExecutionStateSchema).
		Where(WorkflowExecutionStateSchema.RunID.Eq(runID)).
		ForUpdate())
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("workflow execution state %s: %w", runID, execution.ErrRunNotFound)
	}
	state, err := executionStateRowToState(rows[0])
	if err != nil {
		return nil, err
	}
	t.keepState(state)
	return state, nil
}

// keepState 记下本事务持有行锁的 state，CreateRun 新建的 state 同样记在这里。
func (t *gormExecutionTx) keepState(state *execution.WorkflowExecutionState) {
	if t.states == nil {
		t.states = make(map[string]*execution.WorkflowExecutionState)
	}
	t.states[state.RunID] = state
}

// flushStates 把本事务改过的 execution state 各写回一次（每个 Run 一条 UPDATE，没有改动的不写），在提交前调用。
// 行锁都已持有，写回顺序不影响加锁顺序。
func (t *gormExecutionTx) flushStates() error {
	for _, runID := range slices.Sorted(maps.Keys(t.states)) {
		if err := updateExecutionState(t.db, t.states[runID]); err != nil {
			return err
		}
	}
	return nil
}

// appendEvent 在本事务持有行锁的 execution state 上追加一个事件：插入事件行，把事件投影到 state（随 flushStates 写回）。
func (t *gormExecutionTx) appendEvent(runID, treeID, branchID string, event history.Event, dedupeKey *string) (int64, bool, error) {
	s, db := t.store, t.db
	state, err := t.lockedState(runID)
	if err != nil {
		return 0, false, err
	}
	// 终态之后只认已记录过的去重事件（重复的终态写入等），其余追加拒绝。
	if state.IsTerminal() {
		if dedupeKey != nil {
			if id, found, err := findDedupedEventID(db, branchID, *dedupeKey); err != nil || found {
				return id, false, err
			}
		}
		return 0, false, execution.RunClosedError(runID, event)
	}
	// 两个后端的写入顺序相同：没有时间的事件取仓储时钟的当前时刻，归一化失败载荷，带上 ID，编码成 JSON。
	// 存下的就是这份 JSON，event_size_bytes 与历史限额记账用的是它的长度，读回的事件与它一一对应。
	event, err = execution.NormalizeEventFailures(execution.StampEvent(event, s.clock.Now()))
	if err != nil {
		return 0, false, err
	}
	event.ID = state.NextEventID
	eventJSON, err := history.CanonicalEventJSON(event)
	if err != nil {
		return 0, false, err
	}
	size := int64(len(eventJSON))
	if err := history.CheckHistoryBudget(history.HistoryMetrics{EventCount: state.HistoryEventCount, SizeBytes: state.HistorySizeBytes}, []int64{size}, s.historyLimits, history.UsesReservedHistoryCloseBudget(event)); err != nil {
		return 0, false, err
	}
	row := eventToRow(runID, treeID, branchID, event, eventJSON, dedupeKey)
	// 撞唯一键时不写入，下面按 RowsAffected 判断 dedupe 是否命中。
	result := db.Clauses(onDuplicateKeyNoop("id")).Create(row)
	if result.Error != nil {
		return 0, false, result.Error
	}
	if result.RowsAffected == 0 {
		// node_id 取自持有行锁的 execution state，(branch_id, node_id) 不会重复；没有写入说明 dedupe 命中。
		if dedupeKey == nil {
			return 0, false, fmt.Errorf("append event %s/%d: node already exists", branchID, event.ID)
		}
		id, err := dedupedEventID(db, branchID, *dedupeKey)
		return id, false, err
	}
	execution.ApplyEventToExecutionState(state, event, size)
	// retention GC 投递点。三个约束缺一不可：
	//
	//  1. **必须在这里，不能在 execution.ApplyEventToExecutionState 内**：那是个纯内存函数
	//     （没有 tx 句柄可用），且位于 BuildWorkflowExecutionState 的**重放路径**上被反复
	//     调用。在那里投递会给已经删掉的 run 凭空复活删除任务。
	//  2. **必须门控在真正新插入之后**：上面 result.RowsAffected == 0 的分支已提前 return，
	//     走到这里即 inserted == true。重放/幂等重放走不到这里，天然不重排。
	//  3. **必须在事务内**：appendEvent 只在 gormExecutionTx 的事务里执行，
	//     故投递失败会让终态事件一起回滚。反之若在事务外投递，就存在「关闭成功但任务
	//     没排上」的窗口，那棵 tree 永久泄漏。
	if s.retention.enabled() && isTerminalEvent(event) {
		namespace, err := runNamespace(db, runID)
		if err != nil {
			return 0, false, err
		}
		if err := enqueueDeletionTask(db, treeID, namespace, s.retention.fireAt(event.Timestamp), s.clock.Now()); err != nil {
			return 0, false, fmt.Errorf("enqueue deletion task for tree %s: %w", treeID, err)
		}
	}
	return event.ID, true, nil
}

// runNamespace 读取 run 的 namespace，供 deletion task 记录归属。查不到时回落到默认
// namespace：namespace 只是运维可读字段，不参与删除逻辑的正确性。
func runNamespace(db *gorm.DB, runID string) (string, error) {
	row, err := gsql.SelectG[RunPO](RunSchema.Namespace).
		From(RunSchema).
		Where(RunSchema.ID.Eq(runID)).Take(db)
	if err := runRowError(runID, row, err); err != nil {
		return "", err
	}
	return row.Namespace, nil
}

// dedupedEventID 返回 branch 上已有的同 dedupe key 事件的 node_id。加锁读取最新已提交的行，
// 不受事务较早建立的一致性读快照影响。
func dedupedEventID(db *gorm.DB, branchID, dedupeKey string) (int64, error) {
	id, found, err := findDedupedEventID(db, branchID, dedupeKey)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("append event %s: dedupe key %q conflicted but no event holds it", branchID, dedupeKey)
	}
	return id, nil
}

// findDedupedEventID 加锁读取 branch 上持有 dedupeKey 的事件的 node_id，found=false 表示没有这样的事件。
func findDedupedEventID(db *gorm.DB, branchID, dedupeKey string) (int64, bool, error) {
	existing, err := gsql.From[EventPO](EventSchema).
		Where(
			EventSchema.BranchID.Eq(branchID),
			EventSchema.DedupeKey.Eq(&dedupeKey),
		).ForUpdate().Take(db)
	if err != nil || existing == nil {
		return 0, false, err
	}
	return existing.NodeID, true, nil
}
