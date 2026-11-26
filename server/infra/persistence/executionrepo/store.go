package executionrepo

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
	"gorm.io/gorm"
)

type Config struct {
	HistoryLimits       history.HistoryLimits
	HistoryPageTokenKey []byte
	Retention           RetentionConfig
	// Clock 是仓储的时间来源：事件时间、execution state 的时间字段与分页令牌有效期都取自它。留空时使用系统时钟。
	Clock clock.Clock
}

var (
	_ execution.ExecutionManager            = (*Repository)(nil)
	_ execution.EventStore                  = (*Repository)(nil)
	_ execution.HistoryBranchStore          = (*Repository)(nil)
	_ execution.HistoryPageStore            = (*Repository)(nil)
	_ execution.ActivityStateStore          = (*Repository)(nil)
	_ execution.TimerStateStore             = (*Repository)(nil)
	_ execution.ChildWorkflowStateStore     = (*Repository)(nil)
	_ execution.WorkflowExecutionStateStore = (*Repository)(nil)
)

type Repository struct {
	db                  infradb.DB
	historyLimits       history.HistoryLimits
	historyPageTokenKey []byte
	retention           RetentionConfig
	clock               clock.Clock
}

func New(database infradb.DB, cfg Config) (*Repository, error) {
	if database == nil {
		return nil, fmt.Errorf("workflow run repository: db is required")
	}
	limits := cfg.HistoryLimits
	if limits.HardEvents == 0 {
		limits = history.DefaultHistoryLimits()
	}
	if err := limits.Validate(); err != nil {
		return nil, fmt.Errorf("workflow run repository history limits: %w", err)
	}
	tokenKey := append([]byte(nil), cfg.HistoryPageTokenKey...)
	if len(tokenKey) == 0 {
		tokenKey = make([]byte, 32)
		if _, err := rand.Read(tokenKey); err != nil {
			return nil, fmt.Errorf("workflow run repository history token key: %w", err)
		}
	}
	if len(tokenKey) < 32 {
		return nil, fmt.Errorf("workflow run repository history token key must contain at least 32 bytes")
	}
	if err := cfg.Retention.Validate(); err != nil {
		return nil, fmt.Errorf("workflow run repository retention: %w", err)
	}
	clk := cfg.Clock
	if clk == nil {
		clk = systemclock.Clock{}
	}
	return &Repository{
		db: database, historyLimits: limits, historyPageTokenKey: tokenKey, retention: cfg.Retention, clock: clk,
	}, nil
}

func (s *Repository) Get(runID string) (*execution.Run, error) {
	row, err := gsql.From[RunPO](RunSchema).
		Where(RunSchema.ID.Eq(runID)).Take(s.db)
	if err := runRowError(runID, row, err); err != nil {
		return nil, err
	}
	return rowToRun(row)
}

// runRowError 把 Run 行不存在统一成 execution.ErrRunNotFound，gorm 的错误类型留在仓储内。
func runRowError(runID string, row *RunPO, err error) error {
	if err == nil && row == nil {
		return fmt.Errorf("run %s: %w", runID, execution.ErrRunNotFound)
	}
	return err
}

// SetResetRunID 把 reset_run_id 写成最新一次 reset 的新 Run，同一个 base Run 可以多次 reset，
// 后写覆盖先写；base Run 不存在时返回 execution.ErrRunNotFound。
func (s *Repository) SetResetRunID(runID, resetRunID string) error {
	return s.inTx(func(tx *gormExecutionTx) error { return tx.SetResetRunID(runID, resetRunID) })
}

// GetCurrentRunID 查 (namespace,workflowID) 的 is_current=true run（不限 status——current 可能是终态）。
func (s *Repository) GetCurrentRunID(namespace, workflowID string) (string, error) {
	row, err := gsql.SelectG[RunPO](RunSchema.ID).
		From(RunSchema).
		Where(
			RunSchema.Namespace.Eq(protocol.NormalizeNamespace(namespace)),
			RunSchema.WorkflowID.Eq(workflowID),
			RunSchema.IsCurrent.Eq(true),
		).Take(s.db)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", execution.ErrNoCurrentRun
	}
	return row.ID, nil
}

type runCountRow struct {
	Namespace string
	Status    string
	Count     int64
}

// CountRuns 按 (namespace, status) 分组计数，走 idx_workflow_runs_namespace_status 的索引扫描；给出 namespace 时只扫
// 这个 namespace 的索引区间。
func (s *Repository) CountRuns(namespace string) ([]execution.RunCount, error) {
	query := gsql.SelectG[runCountRow](
		RunSchema.Namespace,
		RunSchema.Status,
		RunSchema.ID.Count().As("count"),
	).From(RunSchema)
	if namespace != "" {
		query = query.Where(RunSchema.Namespace.Eq(namespace))
	}
	rows, err := infradb.Find(s.db, query.GroupBy(RunSchema.Namespace, RunSchema.Status))
	if err != nil {
		return nil, err
	}
	counts := make([]execution.RunCount, 0, len(rows))
	for _, row := range rows {
		counts = append(counts, execution.RunCount{
			Namespace: row.Namespace,
			Status:    history.Status(row.Status),
			Count:     int(row.Count),
		})
	}
	return counts, nil
}

type targetWorkerRow struct {
	Namespace            string
	TargetWorkerIdentity string
}

// ListRunningTargetWorkers 走 idx_workflow_runs_status 的 running 区间。
func (s *Repository) ListRunningTargetWorkers() ([]execution.TargetWorker, error) {
	rows, err := infradb.Find(s.db, gsql.SelectG[targetWorkerRow](RunSchema.Namespace, RunSchema.TargetWorkerIdentity).
		From(RunSchema).
		Where(RunSchema.Status.Eq(string(history.StatusRunning)), RunSchema.TargetWorkerIdentity.Not("")).
		Distinct())
	if err != nil {
		return nil, err
	}
	targets := make([]execution.TargetWorker, 0, len(rows))
	for _, row := range rows {
		targets = append(targets, execution.TargetWorker{Namespace: row.Namespace, Identity: row.TargetWorkerIdentity})
	}
	return targets, nil
}

func runRowsToDomain(rows []*RunPO) ([]*execution.Run, error) {
	runs := make([]*execution.Run, 0, len(rows))
	for _, row := range rows {
		run, err := rowToRun(row)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

var terminalRunStatuses = []string{
	string(history.StatusCompleted),
	string(history.StatusFailed),
	string(history.StatusCancelled),
	string(history.StatusTerminated),
	string(history.StatusContinuedAsNew),
}

// List 在分页之前应用全部过滤条件，Total 是过滤后的总数（SkipTotal 时不计数）。排序键是 (created_at, id)，
// created_at 相同的 Run 跨页不重复也不丢失。
//
// blocked 与搜索属性取自 tenuto_execution_states 的投影列，经 EXISTS 子查询下推。
// 两种方言都能表达的属性条件走 SQL；其余条件（见 splitSearchAttributeFilter）走 listByAttributeScan。
func (s *Repository) List(filter execution.StoreListFilter) (execution.StoreListResult, error) {
	runConds := []gsql.Expression{RunSchema.ID.In(filter.RunIDs...)}
	if filter.Namespace != "" {
		runConds = append(runConds, RunSchema.Namespace.Eq(filter.Namespace))
	}
	if filter.WorkflowID != "" {
		runConds = append(runConds, RunSchema.WorkflowID.Eq(filter.WorkflowID))
	}
	if len(filter.Statuses) > 0 {
		vals := make([]string, len(filter.Statuses))
		for i, st := range filter.Statuses {
			vals[i] = string(st)
		}
		runConds = append(runConds, RunSchema.Status.In(vals...))
	} else if !filter.IncludeTerminal {
		runConds = append(runConds, RunSchema.Status.NotIn(terminalRunStatuses...))
	}
	if filter.CreatedBefore != nil {
		runConds = append(runConds, RunSchema.CreatedAt.Lt(*filter.CreatedBefore))
	}
	if after := filter.After; after != nil {
		// 位于游标之后：created_at 更晚，或 created_at 相同而 id 更大（降序时反向）。
		createdAfter, idAfter := RunSchema.CreatedAt.Gt(after.CreatedAt), gsql.Expr("? > ?", RunSchema.ID, after.RunID)
		if filter.OrderDesc {
			createdAfter, idAfter = RunSchema.CreatedAt.Lt(after.CreatedAt), gsql.Expr("? < ?", RunSchema.ID, after.RunID)
		}
		runConds = append(runConds, gsql.Or(createdAfter, gsql.And(RunSchema.CreatedAt.Eq(after.CreatedAt), idAfter)))
	}

	states := WorkflowExecutionStateSchema
	var stateConds []gsql.Expression
	if filter.BlockedOnly {
		stateConds = append(stateConds, states.Blocked.Eq(true))
	}
	sqlAttributes, scanAttributes := splitSearchAttributeFilter(filter.SearchAttributes)
	for _, key := range slices.Sorted(maps.Keys(sqlAttributes)) {
		stateConds = append(stateConds, searchAttributeCondition(key, sqlAttributes[key]))
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if len(scanAttributes) > 0 {
		return s.listByAttributeScan(runConds, stateConds, scanAttributes, filter, limit)
	}

	query := gsql.From[RunPO](RunSchema).Where(runConds...)
	if len(stateConds) > 0 {
		query = query.Where(gsql.Exists(
			gsql.Select(gsql.Lit(1).As("_")).
				From(states).
				Where(states.RunID.EqF(RunSchema.ID)).
				Where(stateConds...),
		))
	}
	var total int64
	if !filter.SkipTotal {
		var err error
		if total, err = query.Count(s.db); err != nil {
			return execution.StoreListResult{}, err
		}
	}
	// 多取一行判断是否还有下一页。
	rows, err := infradb.Find(s.db, query.Order(RunSchema.CreatedAt, !filter.OrderDesc).
		Order(RunSchema.ID, !filter.OrderDesc).
		Offset(filter.Offset).Limit(limit+1))
	if err != nil {
		return execution.StoreListResult{}, err
	}
	hasMore := len(rows) > limit
	runs, err := runRowsToDomain(rows[:min(len(rows), limit)])
	if err != nil {
		return execution.StoreListResult{}, err
	}
	return execution.StoreListResult{Runs: runs, Total: int(total), HasMore: hasMore}, nil
}

// splitSearchAttributeFilter 把属性条件分成 SQL 可表达的全等条件与需要在 Go 中判定的条件。
//
// 走 Go 判定的两类：ChangeVersionSearchAttribute 的值是 JSON 数组编码成的字符串，成员匹配要把
// 字符串再解析成数组；键含引号、反斜杠或控制字符时，拼进 JSON path 需要额外转义。
func splitSearchAttributeFilter(attributes map[string]string) (sqlAttributes, scanAttributes map[string]string) {
	sqlAttributes, scanAttributes = map[string]string{}, map[string]string{}
	for key, value := range attributes {
		unsafePath := key == "" || strings.ContainsFunc(key, func(r rune) bool { return r == '"' || r == '\\' || r < 0x20 })
		if key == history.ChangeVersionSearchAttribute || unsafePath {
			scanAttributes[key] = value
			continue
		}
		sqlAttributes[key] = value
	}
	return sqlAttributes, scanAttributes
}

// searchAttributeCondition 表达 attrs[key] == value，缺失的键等同空串（与 execution.MatchSearchAttributes 一致）。
// 列类型是 LONGBLOB，先 CAST 成字符再交给 JSON_EXTRACT。
func searchAttributeCondition(key, value string) gsql.Expression {
	extracted := gsql.JsonOf(WorkflowExecutionStateSchema.SearchAttributesJSON.CastChar()).Extract(`$."` + key + `"`)
	if value == "" {
		return gsql.Or(extracted.IsNull(), extracted.Eq(""))
	}
	return extracted.Eq(value)
}

type runAttributesRow struct {
	RunID                string `gorm:"column:run_id"`
	SearchAttributesJSON []byte `gorm:"column:search_attributes_json"`
}

// listByAttributeScan 按排序取出候选 Run 的 (run_id, 搜索属性)，用 execution.MatchSearchAttributes
// 过滤后再分页，页内容与 Total 都来自过滤后的集合。候选集已经过全部 SQL 条件收窄。
func (s *Repository) listByAttributeScan(runConds, stateConds []gsql.Expression, attributes map[string]string, filter execution.StoreListFilter, limit int) (execution.StoreListResult, error) {
	states := WorkflowExecutionStateSchema
	candidates, err := infradb.Find(s.db, gsql.SelectG[runAttributesRow](states.RunID, states.SearchAttributesJSON).
		From(states).
		Join(gsql.InnerJoin(RunSchema).On(RunSchema.ID.EqF(states.RunID))).
		Where(runConds...).
		Where(stateConds...).
		Order(RunSchema.CreatedAt, !filter.OrderDesc).
		Order(RunSchema.ID, !filter.OrderDesc))
	if err != nil {
		return execution.StoreListResult{}, err
	}
	matched := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		var attrs map[string]string
		if len(candidate.SearchAttributesJSON) > 0 {
			if err := json.Unmarshal(candidate.SearchAttributesJSON, &attrs); err != nil {
				return execution.StoreListResult{}, fmt.Errorf("run %s search attributes: %w", candidate.RunID, err)
			}
		}
		if execution.MatchSearchAttributes(attrs, attributes) {
			matched = append(matched, candidate.RunID)
		}
	}
	result := execution.StoreListResult{Total: len(matched)}
	if filter.SkipTotal {
		result.Total = 0
	}
	page := matched[min(max(filter.Offset, 0), len(matched)):]
	result.HasMore = len(page) > limit
	page = page[:min(limit, len(page))]
	if len(page) == 0 {
		return result, nil
	}
	rows, err := infradb.Find(s.db, gsql.From[RunPO](RunSchema).Where(RunSchema.ID.In(page...)))
	if err != nil {
		return execution.StoreListResult{}, err
	}
	runs, err := runRowsToDomain(rows)
	if err != nil {
		return execution.StoreListResult{}, err
	}
	position := make(map[string]int, len(page))
	for i, runID := range page {
		position[runID] = i
	}
	slices.SortFunc(runs, func(a, b *execution.Run) int { return position[a.ID] - position[b.ID] })
	result.Runs = runs
	return result, nil
}

// ── EventStore ──

func (s *Repository) Append(runID, treeID, branchID string, event history.Event) (int64, error) {
	var eventID int64
	err := s.inTx(func(tx *gormExecutionTx) error {
		var err error
		eventID, err = tx.Append(runID, treeID, branchID, event)
		return err
	})
	return eventID, err
}

func (s *Repository) AppendIdempotent(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	var eventID int64
	var inserted bool
	err := s.inTx(func(tx *gormExecutionTx) error {
		var err error
		eventID, inserted, err = tx.AppendIdempotent(runID, treeID, branchID, event)
		return err
	})
	return eventID, inserted, err
}

func (s *Repository) AppendWorkflowTaskScheduled(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if event.WorkflowTaskScheduled == nil {
		event.WorkflowTaskScheduled = &history.WorkflowTaskScheduledEvent{}
	}
	var eventID int64
	var inserted bool
	err := s.inTx(func(tx *gormExecutionTx) error {
		// 死锁重放时上一次尝试的结果作废。
		eventID, inserted = 0, false
		// open 的 Workflow Task 由 execution state 记录，在持有状态行锁的事务内判定并追加。
		state, err := tx.lockedState(runID)
		if err != nil {
			return err
		}
		if state.OpenWorkflowTaskEventID > 0 {
			eventID = state.OpenWorkflowTaskEventID
			return nil
		}
		eventID, inserted, err = tx.appendEvent(runID, treeID, branchID, event, nil)
		return err
	})
	return eventID, inserted, err
}

func (s *Repository) GetWorkflowTaskScheduledEvent(runID string, scheduledEventID int64) (history.Event, bool, error) {
	row, err := gsql.From[EventPO](EventSchema).
		Where(
			EventSchema.RunID.Eq(runID),
			EventSchema.NodeID.Eq(scheduledEventID),
			EventSchema.Type.Eq(string(history.EventWorkflowTaskScheduled)),
		).Take(s.db)
	if err != nil {
		return history.Event{}, false, err
	}
	if row == nil {
		return history.Event{}, false, nil
	}
	event, err := rowToEvent(row)
	if err != nil {
		return history.Event{}, false, err
	}
	return *event, true, nil
}

func (s *Repository) GetEventIDByDedupeKey(_, branchID, dedupeKey string) (int64, bool, error) {
	row, err := gsql.From[EventPO](EventSchema).
		Where(
			EventSchema.BranchID.Eq(branchID),
			EventSchema.DedupeKey.Eq(&dedupeKey),
		).Take(s.db)
	if err != nil || row == nil {
		return 0, false, err
	}
	return row.NodeID, true, nil
}

func (s *Repository) AppendWorkflowTaskClosed(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	var eventID int64
	var inserted bool
	err := s.RunInTx(func(tx execution.ExecutionTx) error {
		id, ok, err := tx.AppendWorkflowTaskClosed(runID, treeID, branchID, event)
		eventID, inserted = id, ok
		return err
	})
	return eventID, inserted, err
}

func (s *Repository) AppendWorkflowTaskCompletedWithEvents(runID, treeID, branchID string, closeEvent history.Event, commandEvents []history.Event) (execution.WorkflowTaskCompletionResult, error) {
	if closeEvent.EventType() != history.EventWorkflowTaskCompleted {
		return execution.WorkflowTaskCompletionResult{}, fmt.Errorf("workflow task completed event type required: %s", closeEvent.EventType())
	}
	var result execution.WorkflowTaskCompletionResult
	err := s.inTx(func(tx *gormExecutionTx) error {
		var err error
		result, err = tx.appendWorkflowTaskCompleted(runID, treeID, branchID, closeEvent, commandEvents)
		return err
	})
	return result, err
}

// appendWorkflowTaskCompleted 在本事务里追加 WFT close（释放 open 槽）与命令事件（幂等），顶层方法与 RunInTx 共用。
// 调用方负责校验 closeEvent 类型。
func (t *gormExecutionTx) appendWorkflowTaskCompleted(runID, treeID, branchID string, closeEvent history.Event, commandEvents []history.Event) (execution.WorkflowTaskCompletionResult, error) {
	result := execution.WorkflowTaskCompletionResult{
		CommandEvents: make([]execution.WorkflowTaskCommandEventResult, 0, len(commandEvents)),
	}
	// 同一次提交的事件共用一个时刻。
	now := t.store.clock.Now()
	closeEventID, inserted, err := t.AppendWorkflowTaskClosed(runID, treeID, branchID, execution.StampEvent(closeEvent, now))
	if err != nil {
		return result, err
	}
	if !inserted {
		result.ClosedInserted = false
		return result, nil
	}
	result.ClosedInserted = true
	result.ClosedEventID = closeEventID
	for _, event := range commandEvents {
		var key *string
		if dedupeKey, ok := history.DedupeKey(event); ok {
			key = &dedupeKey
		}
		cmdNodeID, cmdInserted, err := t.appendEvent(runID, treeID, branchID, execution.StampEvent(event, now), key)
		if err != nil {
			return result, err
		}
		result.CommandEvents = append(result.CommandEvents, execution.WorkflowTaskCommandEventResult{
			EventID:  cmdNodeID,
			Inserted: cmdInserted,
		})
	}
	return result, nil
}

func (s *Repository) RunInTx(fn func(tx execution.ExecutionTx) error) error {
	return s.inTx(func(tx *gormExecutionTx) error { return fn(tx) })
}

// maxDeadlockAttempts 是一个事务体因 InnoDB 死锁被回滚后的总执行次数上限。
const maxDeadlockAttempts = 3

// inTx 开一个事务并把事务句柄包成 gormExecutionTx，顶层方法与 RunInTx 共用同一份事务体。fn 成功后、提交之前把
// 本事务改过的 execution state 写回（flushStates）。
//
// InnoDB 选中本事务作为死锁的牺牲者（Error 1213）时整个事务已回滚，inTx 用新事务把 fn 整体再执行一次，最多
// maxDeadlockAttempts 次。fn 是可重放的闭包（见 execution.ExecutionManager.RunInTx），每次执行都从新的
// gormExecutionTx 开始；fn 写到外层的结果变量在闭包开头重置，重放走了另一条分支时不残留上一次尝试的值。
func (s *Repository) inTx(fn func(tx *gormExecutionTx) error) error {
	for attempt := 1; ; attempt++ {
		err := s.db.Transaction(func(db *gorm.DB) error {
			tx := &gormExecutionTx{store: s, db: db}
			if err := fn(tx); err != nil {
				return err
			}
			return tx.flushStates()
		})
		if attempt >= maxDeadlockAttempts || !infradb.IsDeadlock(err) {
			return err
		}
	}
}

// gormExecutionTx 把事务内的 history append 与 state 写入路由到同一个 *gorm.DB(tx)，保证单事务原子。
// states 是本事务加锁读到的 execution state（见 lockedState）。
type gormExecutionTx struct {
	store  *Repository
	db     *gorm.DB
	states map[string]*execution.WorkflowExecutionState
}

func (t *gormExecutionTx) AppendIdempotent(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	var key *string
	if dedupeKey, ok := history.DedupeKey(event); ok {
		key = &dedupeKey
	}
	return t.appendEvent(runID, treeID, branchID, event, key)
}

func (t *gormExecutionTx) Append(runID, treeID, branchID string, event history.Event) (int64, error) {
	id, _, err := t.appendEvent(runID, treeID, branchID, event, nil)
	return id, err
}

func (t *gormExecutionTx) AppendWorkflowTaskClosed(runID, treeID, branchID string, event history.Event) (int64, bool, error) {
	if !history.IsWorkflowTaskCloseEventType(event.EventType()) {
		return 0, false, fmt.Errorf("workflow task close event type required: %s", event.EventType())
	}
	closeKey := workflowTaskClosedDedupeKey(event.FlatScheduledEventID())
	return t.appendEvent(runID, treeID, branchID, event, &closeKey)
}

func (t *gormExecutionTx) AppendWorkflowTaskCompleted(runID, treeID, branchID string, closeEvent history.Event, commandEvents []history.Event) (execution.WorkflowTaskCompletionResult, error) {
	if closeEvent.EventType() != history.EventWorkflowTaskCompleted {
		return execution.WorkflowTaskCompletionResult{}, fmt.Errorf("workflow task completed event type required: %s", closeEvent.EventType())
	}
	return t.appendWorkflowTaskCompleted(runID, treeID, branchID, closeEvent, commandEvents)
}

func (t *gormExecutionTx) UpsertActivityState(state *execution.ActivityState) error {
	return t.store.upsertActivityState(t.db, state)
}

func (t *gormExecutionTx) UpdateActivityState(state *execution.ActivityState) error {
	return t.store.updateActivityState(t.db, state)
}

// GetActivityStateForUpdate 在事务内带行级锁读 state（SELECT ... FOR UPDATE），
// 事务内"读+校验+写"串行化，
// 杜绝并发 Handle* 的读-改-写 TOCTOU（🔴-3 方案 b）。
func (t *gormExecutionTx) GetActivityStateForUpdate(runID string, scheduledEventID int64) (*execution.ActivityState, bool, error) {
	rows, err := infradb.Find(t.db, gsql.From[ActivityStatePO](ActivityStateSchema).
		Where(ActivityStateSchema.RunID.Eq(runID), ActivityStateSchema.ScheduledEventID.Eq(scheduledEventID)).
		ForUpdate())
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	st, err := activityStateRowToState(rows[0])
	if err != nil {
		return nil, false, err
	}
	return st, true, nil
}

func (t *gormExecutionTx) UpsertTimerState(state *execution.TimerState) error {
	return t.store.upsertTimerState(t.db, state)
}

func (t *gormExecutionTx) UpdateTimerState(state *execution.TimerState) error {
	return updateTimerState(t.db, state)
}

// GetTimerStateForUpdate 在事务内带行级锁读 timer state（SELECT ... FOR UPDATE），
// TimerFired 在 RunInTx 内先读再校验非终态再写，杜绝重复 fire 竞态（照 activity 模式）。
func (t *gormExecutionTx) GetTimerStateForUpdate(runID string, scheduledEventID int64) (*execution.TimerState, bool, error) {
	rows, err := infradb.Find(t.db, gsql.From[TimerStatePO](TimerStateSchema).
		Where(TimerStateSchema.RunID.Eq(runID), TimerStateSchema.ScheduledEventID.Eq(scheduledEventID)).
		ForUpdate())
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	state, err := timerStateRowToState(rows[0])
	if err != nil {
		return nil, false, err
	}
	return state, true, nil
}

// CloseScheduledTimers 是一条 UPDATE：runID 仍为 scheduled 的 timer 置为 run_closed。
func (t *gormExecutionTx) CloseScheduledTimers(runID string) error {
	return gsql.From[TimerStatePO](TimerStateSchema).
		Where(TimerStateSchema.RunID.Eq(runID), TimerStateSchema.Status.Eq(string(execution.TimerStatusEnums.Scheduled))).
		Update(t.db, map[string]any{
			"status":     string(execution.TimerStatusEnums.RunClosed),
			"updated_at": t.store.clock.Now(),
		}).Error
}

// CloseOpenActivities 是一条 UPDATE：runID 仍为 scheduled 或 started 的 Activity 置为 run_closed，清掉到期索引
// timeout_at（终态 Activity 没有生效的超时）。
func (t *gormExecutionTx) CloseOpenActivities(runID string) error {
	return gsql.From[ActivityStatePO](ActivityStateSchema).
		Where(ActivityStateSchema.RunID.Eq(runID), ActivityStateSchema.Status.In(
			string(execution.ActivityStatusEnums.Scheduled), string(execution.ActivityStatusEnums.Started))).
		Update(t.db, map[string]any{
			"status":     string(execution.ActivityStatusEnums.RunClosed),
			"timeout_at": nil,
			"updated_at": t.store.clock.Now(),
		}).Error
}

func (t *gormExecutionTx) UpsertChildWorkflowState(state *execution.ChildWorkflowState) error {
	return t.store.upsertChildWorkflowState(t.db, state)
}

// GetChildWorkflowStateForUpdate 在事务内带行级锁读 child state（SELECT ... FOR UPDATE），
// 照 GetActivityStateForUpdate 范式。子→父传播 close 在 RunInTx 内"读+校验非终态+写"串行化，
// 杜绝并发/重复 close 的读-改-写竞态。
func (t *gormExecutionTx) GetChildWorkflowStateForUpdate(runID string, scheduledEventID int64) (*execution.ChildWorkflowState, bool, error) {
	rows, err := infradb.Find(t.db, gsql.From[ChildWorkflowStatePO](ChildWorkflowStateSchema).
		Where(ChildWorkflowStateSchema.RunID.Eq(runID), ChildWorkflowStateSchema.ScheduledEventID.Eq(scheduledEventID)).
		ForUpdate())
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	st, err := childWorkflowStateRowToState(rows[0])
	if err != nil {
		return nil, false, err
	}
	return st, true, nil
}

// CreateRun 在事务内建新 run 与它的 execution state。Run ID 已存在时返回 execution.ErrRunAlreadyExists。
// 引擎创建的 Run 不带 current 标记（current 只经 SetCurrentRunCAS 设置），插入撞唯一键即 ID 重复。
func (t *gormExecutionTx) CreateRun(run *execution.Run) error {
	if len(run.BranchToken) == 0 {
		return fmt.Errorf("run %s: %w", run.ID, execution.ErrRunBranchTokenMissing)
	}
	row, err := runToRow(run)
	if err != nil {
		return err
	}
	if err := t.db.Create(row).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return fmt.Errorf("run %s: %w", run.ID, execution.ErrRunAlreadyExists)
		}
		return err
	}
	events, err := t.store.getEventsWithDB(t.db, run)
	if err != nil {
		return err
	}
	state, err := execution.BuildWorkflowExecutionState(run, events)
	if err != nil {
		return err
	}
	if err := createExecutionState(t.db, state); err != nil {
		return err
	}
	t.keepState(state)
	run.ClearPatch()
	return nil
}

// GetRunForUpdate 先以 FOR UPDATE 读 execution state 行，再以 FOR UPDATE 读 Run 行，语义见 execution.ExecutionTx。
// Run 行用加锁读：加锁读总是读到已提交的最新值，与本事务的一致性读快照何时建立无关。
func (t *gormExecutionTx) GetRunForUpdate(runID string) (*execution.Run, error) {
	if _, err := t.lockedState(runID); err != nil {
		return nil, err
	}
	row, err := gsql.From[RunPO](RunSchema).Where(RunSchema.ID.Eq(runID)).ForUpdate().Take(t.db)
	if err := runRowError(runID, row, err); err != nil {
		return nil, err
	}
	return rowToRun(row)
}

// UpdateRun 在事务内先取 execution state 行锁，再把 run 经 setter 改过的列写回，并同步 state 里来自 Run 的投影
// （随 flushStates 写回）。只写改过的列，其余列保持已提交的值。
func (t *gormExecutionTx) UpdateRun(run *execution.Run) error {
	state, err := t.lockedState(run.ID)
	if err != nil {
		return err
	}
	row, err := runToRow(run)
	if err != nil {
		return err
	}
	changed := run.ExportPatch()
	patch := new(RunPO).ToPatch(run)
	if changed.Namespace.IsPresent() {
		patch["namespace"] = row.Namespace
	}
	if changed.Input.IsPresent() {
		patch["input_json"] = row.InputJSON
	}
	if changed.Output.IsPresent() {
		patch["output_json"] = row.OutputJSON
	}
	if changed.SearchAttributes.IsPresent() {
		patch["search_attributes_json"] = row.SearchAttributesJSON
	}
	// current 只经 SetCurrentRunCAS 切换。
	delete(patch, "is_current")
	if len(patch) > 0 {
		if err := gsql.From[RunPO](RunSchema).Where(RunSchema.ID.Eq(run.ID)).Update(t.db, patch).Error; err != nil {
			return err
		}
	}
	state.SyncFromRun(run, t.store.clock.Now())
	run.ClearPatch()
	return nil
}

// GetWorkflowExecutionStateForUpdate 返回本事务持有行锁的 state 的副本：调用方改动副本不影响本事务要写回的 state。
func (t *gormExecutionTx) GetWorkflowExecutionStateForUpdate(runID string) (*execution.WorkflowExecutionState, error) {
	state, err := t.lockedState(runID)
	if err != nil {
		return nil, err
	}
	return execution.CloneWorkflowExecutionState(state), nil
}

func (t *gormExecutionTx) HistoryLimits() history.HistoryLimits { return t.store.historyLimits }

func (t *gormExecutionTx) BeginWorkflowActivation(runID string, scheduledEventID, executionGeneration, stickyGeneration int64, activationID string) (bool, error) {
	state, err := t.lockedState(runID)
	if err != nil {
		return false, err
	}
	return state.BeginActivation(scheduledEventID, executionGeneration, stickyGeneration, activationID, t.store.clock.Now()), nil
}

// SyncRunSearchAttributes 把事务内已投影的搜索属性写回 Run 行，只更新这一列。
func (t *gormExecutionTx) SyncRunSearchAttributes(runID string) error {
	state, err := t.lockedState(runID)
	if err != nil {
		return err
	}
	attrs, err := marshalStringMap(state.SearchAttributes)
	if err != nil {
		return err
	}
	return gsql.From[RunPO](RunSchema).
		Where(RunSchema.ID.Eq(runID)).
		Update(t.db, map[string]any{"search_attributes_json": attrs}).Error
}

// SetResetRunID 在事务内先取 runID 的 execution state 行锁，再写 reset_run_id，语义同 Repository.SetResetRunID。
// state 行与 Run 行同生同删，取锁时已确认 Run 存在。
func (t *gormExecutionTx) SetResetRunID(runID, resetRunID string) error {
	if _, err := t.lockedState(runID); err != nil {
		return err
	}
	return gsql.From[RunPO](RunSchema).
		Where(RunSchema.ID.Eq(runID)).
		Update(t.db, map[string]any{"reset_run_id": resetRunID}).Error
}

// lockRunStates 依次取 runIDs 的 execution state 行锁；Run 不存在时返回 false。
func (t *gormExecutionTx) lockRunStates(runIDs ...string) (bool, error) {
	for _, runID := range runIDs {
		if _, err := t.lockedState(runID); err != nil {
			if errors.Is(err, execution.ErrRunNotFound) {
				return false, nil
			}
			return false, err
		}
	}
	return true, nil
}

// SetCurrentRunCAS 在事务内真条件切换 current（对标 Temporal assertRunIDAndUpdateCurrentExecution）。
// 写 Run 行之前先取 expectedOld 与 newRunID 的 execution state 行锁。
// WHERE 子句带 compare 条件（id=expectedOld AND is_current=true），不匹配则 RowsAffected=0，据此返回 false。
//
// current_marker 唯一约束兜底：首设路径（expectedOld=""）是 Count+Update 无锁，并发可能 TOCTOU
// 漏判，第二个置 current 写入相同 current_marker 会撞唯一约束 → gorm.ErrDuplicatedKey（依赖连接开启 TranslateError）→ 返回 (false, nil)，
// 保证 DB 层面"每 workflow 至多一个 current"。
func (t *gormExecutionTx) SetCurrentRunCAS(namespace, workflowID, expectedOldRunID, newRunID string) (bool, error) {
	ns := protocol.NormalizeNamespace(namespace)
	locked := []string{newRunID}
	if expectedOldRunID != "" {
		locked = []string{expectedOldRunID, newRunID}
	}
	if ok, err := t.lockRunStates(locked...); err != nil || !ok {
		return false, err
	}
	if expectedOldRunID == "" {
		// 首次设置：仅当该 workflow 当前无 is_current run 时才允许。
		var existing int64
		var err error
		existing, err = gsql.From[RunPO](RunSchema).
			Where(
				RunSchema.Namespace.Eq(ns),
				RunSchema.WorkflowID.Eq(workflowID),
				RunSchema.IsCurrent.Eq(true),
			).
			Count(t.db)
		if err != nil {
			return false, err
		}
		if existing > 0 {
			return false, nil
		}
	} else {
		// CAS compare：只有旧 current 确实是 expectedOld 且仍为 current 时，清旧 current 才会命中。
		// 被别人切走（is_current 已是别的 run）则 RowsAffected=0 → compare 失败。
		clearRes := gsql.From[RunPO](RunSchema).
			Where(
				RunSchema.Namespace.Eq(ns),
				RunSchema.WorkflowID.Eq(workflowID),
				RunSchema.ID.Eq(expectedOldRunID),
				RunSchema.IsCurrent.Eq(true),
			).
			Update(t.db, map[string]any{"is_current": false, "current_marker": nil})
		if clearRes.Error != nil {
			return false, clearRes.Error
		}
		if clearRes.RowsAffected == 0 {
			return false, nil
		}
	}
	// 置新 current：is_current=true 同步写 current_marker。撞唯一约束（并发已有 current）→ CAS 失败。
	marker := currentMarkerValue(ns, workflowID)
	setRes := gsql.From[RunPO](RunSchema).
		Where(
			RunSchema.Namespace.Eq(ns),
			RunSchema.WorkflowID.Eq(workflowID),
			RunSchema.ID.Eq(newRunID),
		).
		Update(t.db, map[string]any{"is_current": true, "current_marker": marker})
	if setRes.Error != nil {
		if errors.Is(setRes.Error, gorm.ErrDuplicatedKey) {
			return false, nil
		}
		return false, setRes.Error
	}
	return setRes.RowsAffected > 0, nil
}

func (s *Repository) GetEvents(runID string) ([]history.Event, error) {
	run, err := s.Get(runID)
	if err != nil {
		return nil, err
	}
	return s.getEventsWithDB(s.db, run)
}

func (s *Repository) getEventsWithDB(db infradb.DB, run *execution.Run) ([]history.Event, error) {
	return s.getEventsRangeWithDB(db, run, 0, 0)
}

// getEventsRangeWithDB 在 getEventsWithDB 的 branch 可见性解析之上，把
// (afterExclusive, throughInclusive] 的范围过滤**下推到 SQL**。
// afterExclusive<=0 表示不限下界，throughInclusive<=0 表示不限上界。
//
// 为什么下推是逐行等价的：execution.Event.ID 就是 EventPO.NodeID（见 rowToEvent），而 branch 祖先链
// 解析产生的也正是一串作用在 node_id 上的 `(branch_id=? AND node_id>=? AND node_id<?)`。
// 二者作用在**同一列**上且互相正交——branch 可见性由 OR 组决定，范围由独立的 AND 项决定，
// 交集与求值顺序无关。故"OR 组 AND 范围项"与"先全量取出再按 ID 过滤"结果完全一致。
//
// 已有索引 idx_workflow_events_branch_node (branch_id, node_id) 覆盖 OR 组的每一支加范围项。
func (s *Repository) getEventsRangeWithDB(db infradb.DB, run *execution.Run, afterExclusive, throughInclusive int64) ([]history.Event, error) {
	branch, err := runBranch(run)
	if err != nil {
		return nil, err
	}

	type branchRange struct {
		treeID   string
		branchID string
		minNode  int64
		maxNode  int64
	}
	ranges := make([]branchRange, 0, len(branch.Ancestors)+1)
	for _, a := range branch.Ancestors {
		ranges = append(ranges, branchRange{
			treeID:   branch.TreeID,
			branchID: a.BranchID,
			minNode:  a.BeginNodeID,
			maxNode:  a.EndNodeID,
		})
	}
	ranges = append(ranges, branchRange{
		treeID:   branch.TreeID,
		branchID: branch.BranchID,
		minNode:  execution.GetBeginNodeID(branch),
		maxNode:  math.MaxInt64,
	})

	query := gsql.From[EventPO](EventSchema).Where(EventSchema.TreeID.Eq(branch.TreeID))
	branchRanges := make([]gsql.Expression, 0, len(ranges))
	for _, r := range ranges {
		conditions := []gsql.Expression{
			EventSchema.BranchID.Eq(r.branchID),
			EventSchema.NodeID.Gte(r.minNode),
		}
		if r.maxNode != math.MaxInt64 {
			conditions = append(conditions, EventSchema.NodeID.Lt(r.maxNode))
		}
		branchRanges = append(branchRanges, gsql.And(conditions...))
	}

	// 范围项作为独立的 AND 项追加在整个 OR 组之外。
	query = query.Where(gsql.Or(branchRanges...))
	if afterExclusive > 0 {
		query = query.Where(EventSchema.NodeID.Gt(afterExclusive))
	}
	if throughInclusive > 0 {
		query = query.Where(EventSchema.NodeID.Lte(throughInclusive))
	}
	rows, err := infradb.Find(db, query.Order(EventSchema.NodeID))
	if err != nil {
		return nil, err
	}

	events := make([]history.Event, 0, len(rows))
	for _, row := range rows {
		event, err := rowToEvent(row)
		if err != nil {
			return nil, err
		}
		events = append(events, *event)
	}
	return events, nil
}

// runBranch 解出 Run 的 history branch。每个 Run 在创建时都带 branch token
// （execution.PrepareStartedRun 与 reset 的 ForkBranch），token 为空说明这条不变式被破坏。
func runBranch(run *execution.Run) (*execution.HistoryBranch, error) {
	if len(run.BranchToken) == 0 {
		return nil, fmt.Errorf("run %s: %w", run.ID, execution.ErrRunBranchTokenMissing)
	}
	return execution.DeserializeBranchToken(run.BranchToken)
}

// ── HistoryBranchStore ──

func (s *Repository) ForkBranch(req execution.ForkBranchRequest) (*execution.ForkBranchResponse, error) {
	sourceRun, err := s.Get(req.SourceRunID)
	if err != nil {
		return nil, fmt.Errorf("fork branch: get source run: %w", err)
	}
	sourceBranch, err := runBranch(sourceRun)
	if err != nil {
		return nil, fmt.Errorf("fork branch: source branch token: %w", err)
	}

	newBranch := execution.ForkHistoryBranch(sourceBranch, req.ForkNodeID, req.NewBranchID)
	now := s.clock.Now()

	dataJSON, err := execution.SerializeBranchToken(newBranch)
	if err != nil {
		return nil, err
	}

	sourceJSON, err := execution.SerializeBranchToken(sourceBranch)
	if err != nil {
		return nil, err
	}

	if err := s.db.Transaction(func(tx *gorm.DB) error {
		// 下面两条 INSERT 落在 LockHistoryTree 锁住的行与间隙上：DeleteTree / DeleteBranch
		// 持锁期间它们排队等待，反过来它们未提交时删除方的加锁读也等待。
		// 源 branch 必须在 history tree 里有一行,否则 DeleteBranch 无从得知它还活着,
		// 会把它当成无人引用而删光历史。tenuto 只在 fork 时写 tree row(不像 Temporal
		// 在 AppendHistoryNodes 的 IsNewBranch 分支就为 root 建行),因此这里为源
		// branch 补写。已存在则不动,避免覆盖它自己的 ancestors。
		if err := tx.Clauses(onDuplicateKeyNoop("tree_id")).Create(&HistoryTreePO{
			TreeID:    sourceBranch.TreeID,
			BranchID:  sourceBranch.BranchID,
			DataJSON:  sourceJSON,
			Info:      "root",
			CreatedAt: now,
		}).Error; err != nil {
			return fmt.Errorf("fork branch: ensure source history tree row: %w", err)
		}

		if err := tx.Create(&HistoryTreePO{
			TreeID:    newBranch.TreeID,
			BranchID:  newBranch.BranchID,
			DataJSON:  dataJSON,
			Info:      req.Info,
			CreatedAt: now,
		}).Error; err != nil {
			return fmt.Errorf("fork branch: create history tree row: %w", err)
		}
		// INSERT 排在 DeleteTree 的 tree 锁之后时，源 run 已被它删除。写入之后用当前读（FOR UPDATE）
		// 确认源 run 仍在，不在则整个 fork 回滚，这棵正被回收的树上不留下新 branch。
		sourceRows, err := infradb.Find(tx, gsql.From[RunPO](RunSchema).
			Where(RunSchema.ID.Eq(req.SourceRunID)).
			ForUpdate())
		if err != nil {
			return fmt.Errorf("fork branch: recheck source run: %w", err)
		}
		if len(sourceRows) == 0 {
			return fmt.Errorf("fork branch: source run %s was deleted with its history tree: %w", req.SourceRunID, execution.ErrRunNotFound)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &execution.ForkBranchResponse{BranchToken: dataJSON}, nil
}

// DeleteBranch 删除一个 branch 及其独占的历史区间，引用计数保证仍被其它 branch
// 引用的区间不被误删。幂等：目标 branch 已不存在时返回 nil。
//
// 分两阶段执行，事务边界的划分是刻意的：
//
//	阶段一（事务内）：对 tree 加行锁 → 算出待删区间 → 删掉目标的 history_tree 行。
//	阶段二（事务外）：按 deleteEventBatch 分批把区间内的事件删掉。
//
// **为什么事件删除必须放在事务外**：几十万事件的单条 DELETE 会让 undo log / binlog
// 暴涨、主从延迟出现尖峰、行锁被长时间持有。分批若仍包在同一个事务里，这些代价一点
// 不减（事务提交前 undo log 不会释放），分批就白做了。
//
// **为什么先删 tree 行是安全的**：引用计数的真源是 history_tree 表 —— 目标行一旦在
// 事务内消失，任何并发/后续的 DeleteBranch 都不会再把它当作"活着的引用"，而本次要删
// 的区间已在同一事务内基于加锁快照算完。反过来若把 tree 行留到最后删，阶段二中途崩溃
// 会留下一个"tree 行还在、历史已被删掉一半"的 branch —— 它对后续 DeleteBranch 而言
// 仍是活引用，会保护住一段已经残缺的历史，那才是真正的损坏。
//
// 崩溃的代价因此收敛为**孤儿事件**（tree 行已没、事件还剩一些）：无人可达，且由
// retention GC 的孤儿兜底扫描（scavengeOrphanEvents）回收。这与整个 GC 方案的核心
// 不变式一致 —— 宁可留下不可访问的孤儿事件，也不要留下可访问但历史残缺的 branch。
func (s *Repository) DeleteBranch(treeID, branchID string) error {
	toDelete, err := s.planBranchDeletion(treeID, branchID)
	if err != nil {
		return err
	}
	// 阶段二：事务外分批删事件。每个区间独立，删到 RowsAffected == 0 为止。
	for _, d := range toDelete {
		if err := s.DeleteEventsBatched(
			EventSchema.TreeID.Eq(treeID),
			EventSchema.BranchID.Eq(d.branchID),
			EventSchema.NodeID.Gte(d.fromNodeID),
		); err != nil {
			return fmt.Errorf("delete branch %s/%s: events: %w", treeID, branchID, err)
		}
	}
	return nil
}

// branchDeleteRange 是一个待删区间：branchID 上 node_id >= fromNodeID 的全部事件。
type branchDeleteRange struct {
	branchID   string
	fromNodeID int64
}

// planBranchDeletion 是 DeleteBranch 的阶段一：加锁算出待删区间并摘掉目标 tree 行。
// 返回的区间供调用方在事务外分批删除。
func (s *Repository) planBranchDeletion(treeID, branchID string) ([]branchDeleteRange, error) {
	var toDelete []branchDeleteRange
	err := s.db.Transaction(func(tx *gorm.DB) error {
		allRows, err := LockHistoryTree(tx, treeID)
		if err != nil {
			return err
		}

		var allBranches []execution.HistoryBranch
		var target *execution.HistoryBranch
		for _, row := range allRows {
			var b execution.HistoryBranch
			if err := json.Unmarshal(row.DataJSON, &b); err != nil {
				return fmt.Errorf("delete branch: unmarshal data_json: %w", err)
			}
			allBranches = append(allBranches, b)
			if b.BranchID == branchID {
				bCopy := b
				target = &bCopy
			}
		}
		// branch 不存在视为已删除:删除是幂等操作,"目标已不存在"即调用者要的终态。
		// GC 依赖删除失败可重试,若此处报错,重试会永久卡死。
		if target == nil {
			return nil
		}

		// usedBranches 记录仍被引用的区间。一个仍存在于 history tree 中的 branch,
		// 其自身全段(直到 math.MaxInt64)都是活的。漏掉这行会导致删除某个
		// reset 叶子时,把仍在使用的 base branch 历史整段删除。
		usedBranches := map[string]int64{}
		for _, b := range allBranches {
			if b.BranchID == branchID {
				continue
			}
			usedBranches[b.BranchID] = math.MaxInt64
			for _, a := range b.Ancestors {
				if existing, ok := usedBranches[a.BranchID]; !ok || a.EndNodeID > existing {
					usedBranches[a.BranchID] = a.EndNodeID
				}
			}
		}

		targetBegin := execution.GetBeginNodeID(target)
		allRanges := make([]execution.HistoryBranchRange, 0, len(target.Ancestors)+1)
		allRanges = append(allRanges, target.Ancestors...)
		allRanges = append(allRanges, execution.HistoryBranchRange{
			BranchID:    target.BranchID,
			BeginNodeID: targetBegin,
			EndNodeID:   math.MaxInt64,
		})

		for i := len(allRanges) - 1; i >= 0; i-- {
			r := allRanges[i]
			if maxEnd, used := usedBranches[r.BranchID]; used {
				// 该 branch 被别人引用:只能从 maxEnd 之后删起,[BeginNodeID, maxEnd)
				// 要留给仍引用它的 branch。maxEnd == MaxInt64 表示被完整引用
				// (它自己还活着),一条都不能删。判断依据是 maxEnd 而非待删区间的
				// EndNodeID。
				if maxEnd != math.MaxInt64 {
					toDelete = append(toDelete, branchDeleteRange{r.BranchID, maxEnd})
				}
				// 更上层的祖先同样被引用,无需继续回溯。
				break
			}
			toDelete = append(toDelete, branchDeleteRange{r.BranchID, r.BeginNodeID})
		}

		// 目标 tree 行在事务内摘掉：引用计数的真源随之更新，后续 DeleteBranch 不会再
		// 把这个已判死刑的 branch 当作活引用。事件留到事务外分批删。
		return gsql.From[HistoryTreePO](HistoryTreeSchema).
			Where(
				HistoryTreeSchema.TreeID.Eq(treeID),
				HistoryTreeSchema.BranchID.Eq(branchID),
			).
			Delete(tx).Error
	})
	if err != nil {
		return nil, err
	}
	return toDelete, nil
}

// defaultDeleteEventBatch 是单条 DELETE 最多删除的事件行数。
//
// 上界的意义：一棵有几十万事件的 tree 若一次性 DELETE，undo log 与 binlog 会瞬间
// 膨胀、主从延迟出现尖峰、行锁被长时间持有。分批把每条语句的代价压成常数，代价是
// 多几次往返 —— 对低频后台 GC 完全划算。
const defaultDeleteEventBatch = 1000

// DeleteEventsBatched 循环 `DELETE ... LIMIT n` 删掉满足 conds 的全部事件，
// 直到某轮 RowsAffected == 0。调用方必须给出至少一个条件。
//
// 循环必然终止：每轮的谓词是同一个范围条件，删掉的行不会再被后续轮次匹配，故
// RowsAffected 单调走向 0，不存在"报了非零却没进展"的死循环。
//
// 中断安全：每轮是独立事务，删一半崩溃只是留下更少的孤儿事件，重跑会从剩下的继续删
// （谓词是幂等的范围条件，不依赖任何游标）。
func (s *Repository) DeleteEventsBatched(conds ...gsql.Expression) error {
	for {
		result := gsql.From[EventPO](EventSchema).
			Where(conds...).
			Limit(defaultDeleteEventBatch).
			Delete(s.db)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
	}
}

// LockHistoryTree 在事务内对一棵树的全部 history_tree 行加 FOR UPDATE 行锁并返回它们。
// DeleteBranch 与 DeleteTree 先取这把锁；ForkBranch 往同一范围 INSERT tree 行，与持锁方互相等待。
// 树上还没有任何 tree 行时 InnoDB 给出间隙锁，它同样挡住 ForkBranch 的 INSERT。
func LockHistoryTree(tx *gorm.DB, treeID string) ([]*HistoryTreePO, error) {
	return infradb.Find(tx, gsql.From[HistoryTreePO](HistoryTreeSchema).
		Where(HistoryTreeSchema.TreeID.Eq(treeID)).
		ForUpdate())
}

// ── Internal helpers ──

func workflowTaskClosedDedupeKey(scheduledEventID int64) string {
	return fmt.Sprintf("workflow_task_closed:%d", scheduledEventID)
}
