package memory

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// EventStore 按 Run 保存已提交的事件。写入经 ExecutionManager 的事务缓冲提交。
type EventStore struct {
	mu     sync.RWMutex
	events map[string][]history.Event
	// nextID 按 runID 维护事件序号：每个 run 的 event id 从 1 独立递增，对标 Temporal
	// 的 per-execution EventId 语义（而非跨 run 全局自增）。
	nextID map[string]int64
	// branchBegin 是 reset 出的 Run 在自己 branch 上的第一个事件 ID；更早的事件是从祖先 fork 来的前缀。
	// 没有记录的 Run 整段历史都在自己的 branch 上。
	branchBegin map[string]int64
}

func NewEventStore() *EventStore {
	return &EventStore{
		events:      make(map[string][]history.Event),
		nextID:      make(map[string]int64),
		branchBegin: make(map[string]int64),
	}
}

// storedEvent 返回 event 经存储编解码后的形态与字节数。gorm 后端把同一份 JSON 存进 event_json 列，
// 两个后端读回的事件因此字段与 Go 类型一致。
func storedEvent(event history.Event) (history.Event, int64, error) {
	data, err := history.CanonicalEventJSON(event)
	if err != nil {
		return history.Event{}, 0, err
	}
	stored, err := history.DecodeEventJSON(data)
	return stored, int64(len(data)), err
}

func (s *EventStore) GetWorkflowTaskScheduledEvent(runID string, scheduledEventID int64) (history.Event, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, event := range s.events[runID] {
		if event.ID == scheduledEventID && event.WorkflowTaskScheduled != nil {
			return event, true, nil
		}
	}
	return history.Event{}, false, nil
}

func (s *EventStore) GetEvents(runID string) ([]history.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]history.Event(nil), s.events[runID]...), nil
}

// branchEvents 返回 Run 在自己 branch 上的事件，不含 reset 时从祖先 fork 来的前缀。
// dedupe 与 Workflow Task close 的幂等判定只看这一段，与 gorm 后端 (branch_id, dedupe_key) 唯一索引的范围一致。
func (s *EventStore) branchEvents(runID string) []history.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	begin := s.branchBegin[runID]
	events := make([]history.Event, 0, len(s.events[runID]))
	for _, event := range s.events[runID] {
		if event.ID >= begin {
			events = append(events, event)
		}
	}
	return events
}

// findDedupe 在 runID 自己 branch 上已提交的事件里找去重键为 key 的事件。
func (s *EventStore) findDedupe(runID, key string) (int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	begin := s.branchBegin[runID]
	for i := range s.events[runID] {
		event := &s.events[runID][i]
		if event.ID < begin {
			continue
		}
		if k, ok := history.DedupeKey(*event); ok && k == key {
			return event.ID, true
		}
	}
	return 0, false
}

func (s *EventStore) getEventsRange(runID string, afterExclusive, throughInclusive int64) []history.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := s.events[runID]
	result := make([]history.Event, 0)
	for _, event := range events {
		// 边界与 gorm 后端的 getEventsRangeWithDB 对齐：afterExclusive<=0 不限下界，
		// throughInclusive<=0 不限上界。内存实现本就是 O(H) 且无 IO，不再进一步优化，
		// 只保证两个后端语义不漂移。
		if event.ID > afterExclusive && (throughInclusive <= 0 || event.ID <= throughInclusive) {
			result = append(result, event)
		}
	}
	return result
}

func (s *EventStore) getEventWindow(runID string, snapshotMax, cursorID int64, direction execution.HistoryDirection, limit int) ([]history.Event, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	events := s.events[runID]
	if snapshotMax == 0 && len(events) > 0 {
		snapshotMax = events[len(events)-1].ID
	}
	result := make([]history.Event, 0, limit)
	if direction == execution.HistoryDirectionDescending {
		for index := len(events) - 1; index >= 0 && len(result) < limit; index-- {
			event := events[index]
			if event.ID > snapshotMax || (cursorID > 0 && event.ID >= cursorID) {
				continue
			}
			result = append(result, event)
		}
		return result, snapshotMax
	}
	for _, event := range events {
		if event.ID > snapshotMax || (cursorID > 0 && event.ID <= cursorID) {
			continue
		}
		result = append(result, event)
		if len(result) == limit {
			break
		}
	}
	return result, snapshotMax
}

// appendPrebuilt 写入一个已带 ID 的事件。
func (s *EventStore) appendPrebuilt(runID string, event history.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[runID] = append(s.events[runID], event)
	if event.ID > s.nextID[runID] {
		s.nextID[runID] = event.ID
	}
}

// forkEventsTo 把源 run 中 ID <= forkNodeID 的事件拷贝到新 run，并把新 run 的 nextID
// 推进到该前缀末尾，使新 run 的 GetEvents 返回截断前缀。memory 按 runID flat 存事件、
// 无 branch 祖先解析，故 fork 需显式拷贝（对标 gorm 后端经 branch 祖先解析的等价效果）。
// 新 run 自己的 branch 从 forkNodeID+1 开始。
func (s *EventStore) forkEventsTo(sourceRunID, newRunID string, forkNodeID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.events[sourceRunID]
	prefix := make([]history.Event, 0, len(src))
	var maxID int64
	for _, e := range src {
		if e.ID <= forkNodeID {
			prefix = append(prefix, e)
			if e.ID > maxID {
				maxID = e.ID
			}
		}
	}
	s.events[newRunID] = prefix
	if maxID > s.nextID[newRunID] {
		s.nextID[newRunID] = maxID
	}
	s.branchBegin[newRunID] = forkNodeID + 1
}

// RunStore 保存 Run 行；workflow 配置与定义版本由内嵌的 DefinitionRepository 保存。
type RunStore struct {
	*DefinitionRepository
	mu   sync.RWMutex
	runs map[string]*execution.Run
}

func NewRunStore() *RunStore {
	return &RunStore{
		DefinitionRepository: NewDefinitionRepository(),
		runs:                 make(map[string]*execution.Run),
	}
}

// create 写入新 Run；ID 已存在时返回 execution.ErrRunAlreadyExists。
func (s *RunStore) create(run *execution.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run = cloneRun(run)
	run.Namespace = protocol.NormalizeNamespace(run.Namespace)
	run.ClearPatch()
	if _, ok := s.runs[run.ID]; ok {
		return fmt.Errorf("run %s: %w", run.ID, execution.ErrRunAlreadyExists)
	}
	s.runs[run.ID] = run
	return nil
}

func (s *RunStore) Get(runID string) (*execution.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, ok := s.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %s: %w", runID, execution.ErrRunNotFound)
	}
	return cloneRun(run), nil
}

// update 把 run 经 setter 改过的字段写到已存的 Run 上，其余字段保持已存的值，与 gorm 的 ToPatch 更新一致。
// current 标记只经 SetCurrentRunCAS 切换。
func (s *RunStore) update(run *execution.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.runs[run.ID]
	if !ok {
		return fmt.Errorf("run %s: %w", run.ID, execution.ErrRunNotFound)
	}
	stored := *existing
	applyPatch(&stored, run, run.ExportPatch())
	stored.IsCurrent = existing.IsCurrent
	updated := cloneRun(&stored)
	updated.ClearPatch()
	s.runs[run.ID] = updated
	return nil
}

// applyPatch 把 patch 里出现的字段从 source 复制到 target。target 与 source 是同一种实体的指针，
// patch 是它的 ExportPatch()：生成的 patch 结构体与实体字段同名。Run、各类 state 与 Schedule 的更新共用它，
// 与 gorm 的 ToPatch 更新一样只写改过的字段。
func applyPatch(target, source, patch any) {
	fields := reflect.ValueOf(patch).Elem()
	from, to := reflect.ValueOf(source).Elem(), reflect.ValueOf(target).Elem()
	for i := 0; i < fields.NumField(); i++ {
		if fields.Field(i).MethodByName("IsPresent").Call(nil)[0].Bool() {
			name := fields.Type().Field(i).Name
			to.FieldByName(name).Set(from.FieldByName(name))
		}
	}
}

// syncSearchAttributes 把 execution state 投影出的搜索属性写回 Run，只改这一个字段。
func (s *RunStore) syncSearchAttributes(runID string, attrs map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if run, ok := s.runs[runID]; ok {
		run.SearchAttributes = cloneStringMap(attrs)
	}
}

func (s *RunStore) SetResetRunID(runID, resetRunID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return fmt.Errorf("run %s: %w", runID, execution.ErrRunNotFound)
	}
	run.ResetRunID = resetRunID
	return nil
}

// GetCurrentRunID 返回 (namespace,workflowID) 的 is_current run（不限 status——current 可能是终态）。
func (s *RunStore) GetCurrentRunID(namespace, workflowID string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ns := protocol.NormalizeNamespace(namespace)
	for _, run := range s.runs {
		if run.IsCurrent && protocol.NormalizeNamespace(run.Namespace) == ns && run.WorkflowID == workflowID {
			return run.ID, nil
		}
	}
	return "", execution.ErrNoCurrentRun
}

// setCurrentRunCAS 是 memExecutionTx commit 阶段调用的真 CAS（持 s.mu，串行）。
// expectedOldRunID=="" 表示首次设置（仅当无 current）；否则当前 current 必须正是 expectedOldRunID 才切。
func (s *RunStore) setCurrentRunCAS(namespace, workflowID, expectedOldRunID, newRunID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := protocol.NormalizeNamespace(namespace)
	var current *execution.Run
	for _, run := range s.runs {
		if run.IsCurrent && protocol.NormalizeNamespace(run.Namespace) == ns && run.WorkflowID == workflowID {
			current = run
			break
		}
	}
	if expectedOldRunID == "" {
		if current != nil {
			return false, nil
		}
	} else {
		if current == nil || current.ID != expectedOldRunID {
			return false, nil
		}
		current.IsCurrent = false
	}
	newRun, ok := s.runs[newRunID]
	if !ok {
		return false, nil
	}
	newRun.IsCurrent = true
	return true, nil
}

// List 在没有 execution state 投影的独立 RunStore 上按 Run 自身的搜索属性过滤，blocked 恒为 false。
func (s *RunStore) List(filter execution.StoreListFilter) (execution.StoreListResult, error) {
	return s.list(filter, func(run *execution.Run) (map[string]string, bool, bool) {
		return run.SearchAttributes, false, true
	})
}

// list 在分页之前应用全部过滤条件；projection 返回 Run 的搜索属性与 blocked 投影，
// ok=false 表示该 Run 没有投影，这类 Run 不满足属性与 blocked 条件。
func (s *RunStore) list(filter execution.StoreListFilter, projection func(run *execution.Run) (attrs map[string]string, blocked, ok bool)) (execution.StoreListResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	terminalStatuses := map[history.Status]bool{
		history.StatusCompleted:      true,
		history.StatusFailed:         true,
		history.StatusCancelled:      true,
		history.StatusTerminated:     true,
		history.StatusContinuedAsNew: true,
	}
	wantedStatuses := make(map[history.Status]bool, len(filter.Statuses))
	for _, st := range filter.Statuses {
		wantedStatuses[st] = true
	}

	var filtered []*execution.Run
	for _, run := range s.runs {
		if len(filter.RunIDs) > 0 && !slices.Contains(filter.RunIDs, run.ID) {
			continue
		}
		ns := protocol.NormalizeNamespace(run.Namespace)
		if filter.Namespace != "" && ns != filter.Namespace {
			continue
		}
		if filter.WorkflowID != "" && run.WorkflowID != filter.WorkflowID {
			continue
		}
		if len(wantedStatuses) > 0 && !wantedStatuses[run.Status] {
			continue
		}
		if len(wantedStatuses) == 0 && !filter.IncludeTerminal && terminalStatuses[run.Status] {
			continue
		}
		if filter.CreatedBefore != nil && !run.CreatedAt.Before(*filter.CreatedBefore) {
			continue
		}
		if filter.After != nil {
			order := cmp.Or(run.CreatedAt.Compare(filter.After.CreatedAt), strings.Compare(run.ID, filter.After.RunID))
			if filter.OrderDesc && order >= 0 || !filter.OrderDesc && order <= 0 {
				continue
			}
		}
		if len(filter.SearchAttributes) > 0 || filter.BlockedOnly {
			attrs, blocked, ok := projection(run)
			if !ok || filter.BlockedOnly && !blocked || !execution.MatchSearchAttributes(attrs, filter.SearchAttributes) {
				continue
			}
		}
		filtered = append(filtered, cloneRun(run))
	}

	// 排序键是 (created_at, id)，与 GORM 实现一致：created_at 相同的 Run 跨页不重复也不丢失。
	slices.SortFunc(filtered, func(a, b *execution.Run) int {
		order := cmp.Or(a.CreatedAt.Compare(b.CreatedAt), strings.Compare(a.ID, b.ID))
		if filter.OrderDesc {
			return -order
		}
		return order
	})

	total := len(filtered)
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := filter.Offset
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	if filter.SkipTotal {
		total = 0
	}
	return execution.StoreListResult{Runs: filtered[offset:end], Total: total}, nil
}

func (s *RunStore) CountRuns() ([]execution.RunCount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	type key struct {
		namespace string
		status    history.Status
	}
	grouped := make(map[key]int)
	for _, run := range s.runs {
		grouped[key{protocol.NormalizeNamespace(run.Namespace), run.Status}]++
	}
	counts := make([]execution.RunCount, 0, len(grouped))
	for group, count := range grouped {
		counts = append(counts, execution.RunCount{Namespace: group.namespace, Status: group.status, Count: count})
	}
	return counts, nil
}

// snapshots 返回全部已提交 Run 的寻址与 current 标记。
func (s *RunStore) snapshots() map[string]runSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := make(map[string]runSnapshot, len(s.runs))
	for id, run := range s.runs {
		snap[id] = newRunSnapshot(run)
	}
	return snap
}

// snapshot 返回一个已提交 Run 的寻址与 current 标记。
func (s *RunStore) snapshot(runID string) (runSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, ok := s.runs[runID]
	if !ok {
		return runSnapshot{}, false
	}
	return newRunSnapshot(run), true
}

func cloneRun(run *execution.Run) *execution.Run {
	if run == nil {
		return nil
	}
	cloned := *run
	cloned.Namespace = protocol.NormalizeNamespace(cloned.Namespace)
	cloned.Input = cloneAnyMap(run.Input)
	cloned.Output = cloneAnyMap(run.Output)
	cloned.SearchAttributes = cloneStringMap(run.SearchAttributes)
	if run.DeadlineAt != nil {
		deadline := *run.DeadlineAt
		cloned.DeadlineAt = &deadline
	}
	if run.RunDeadlineAt != nil {
		rd := *run.RunDeadlineAt
		cloned.RunDeadlineAt = &rd
	}
	return &cloned
}

func cloneAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = cloneAnyValue(value)
	}
	return out
}

func cloneAnyValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return cloneAnyMap(v)
	case map[string]string:
		return cloneStringMap(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneAnyValue(item)
		}
		return out
	default:
		return value
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// TaskQueue 是内存 Workflow 任务队列：一份按入队顺序排列的待领任务，各个出队入口按任务自带的路由字段挑选。
//
//   - Dequeue 取没有定向的任务。
//   - DequeueForWorker 先取 sticky 给这个 Worker 的 Workflow Task，再取普通任务。
type TaskQueue struct {
	mu      sync.Mutex
	pending []protocol.TaskLease
	// leased 按租约 token 保存已出队、尚未确认或退回的任务。Nack 重投这里的副本，不采用调用方交回的
	// lease.Task：Worker 交回的载荷没有签名，不能改写下一次投递（与 TaskQ 重投数据库里的行相同）。
	leased    map[string]protocol.Task
	nextLease uint64
	// changed 在 pending 变化时关闭并换新，唤醒等待中的入队与出队。
	changed chan struct{}
	size    int
}

func NewTaskQueue(size int) *TaskQueue {
	if size <= 0 {
		size = 100
	}
	return &TaskQueue{changed: make(chan struct{}), size: size, leased: make(map[string]protocol.Task)}
}

func (q *TaskQueue) Enqueue(ctx context.Context, task protocol.Task) error {
	for {
		q.mu.Lock()
		if len(q.pending) < q.size {
			q.pending = append(q.pending, protocol.TaskLease{Task: task})
			q.notifyLocked()
			q.mu.Unlock()
			return nil
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// take 取出第一个满足 accept 的任务；prefer 非 nil 时满足 prefer 的任务优先。
func (q *TaskQueue) take(ctx context.Context, prefer, accept func(protocol.Task) bool) (protocol.TaskLease, error) {
	for {
		// 已取消的调用方不再领任务：Worker 的消费循环靠这个错误退出。
		if err := ctx.Err(); err != nil {
			return protocol.TaskLease{}, err
		}
		q.mu.Lock()
		index := -1
		if prefer != nil {
			index = slices.IndexFunc(q.pending, func(lease protocol.TaskLease) bool { return prefer(lease.Task) })
		}
		if index < 0 {
			index = slices.IndexFunc(q.pending, func(lease protocol.TaskLease) bool { return accept(lease.Task) })
		}
		if index >= 0 {
			lease := q.pending[index]
			q.pending = slices.Delete(q.pending, index, index+1)
			q.nextLease++
			lease.Token = fmt.Sprintf("memory-lease-%d", q.nextLease)
			q.leased[lease.Token] = lease.Task
			q.notifyLocked()
			q.mu.Unlock()
			return lease, nil
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return protocol.TaskLease{}, ctx.Err()
		}
	}
}

func (q *TaskQueue) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func untargeted(task protocol.Task) bool {
	return task.TargetWorkerIdentity == "" && task.WorkflowTaskRoute != protocol.WorkflowTaskRouteSticky
}

func (q *TaskQueue) Dequeue(ctx context.Context) (protocol.TaskLease, error) {
	return q.take(ctx, nil, untargeted)
}

func (q *TaskQueue) EnqueueSticky(ctx context.Context, workerIdentity string, task protocol.Task) error {
	if workerIdentity == "" {
		return fmt.Errorf("sticky workflow task worker identity is empty")
	}
	task.WorkflowTaskRoute = protocol.WorkflowTaskRouteSticky
	task.StickyTarget = workerIdentity
	return q.Enqueue(ctx, task)
}

func (q *TaskQueue) DequeueForWorker(ctx context.Context, workerIdentity string) (protocol.TaskLease, error) {
	if workerIdentity == "" {
		return q.Dequeue(ctx)
	}
	sticky := func(task protocol.Task) bool {
		return task.WorkflowTaskRoute == protocol.WorkflowTaskRouteSticky && task.StickyTarget == workerIdentity
	}
	return q.take(ctx, sticky, untargeted)
}

// HandlesStickyRoutingDeadline 返回 false：sticky 期限到期后由引擎释放派发记录并重新投递。
func (q *TaskQueue) HandlesStickyRoutingDeadline() bool { return false }

// Durable 返回 false：任务只在进程内存里。
func (q *TaskQueue) Durable() bool { return false }

func (q *TaskQueue) Ack(ctx context.Context, lease protocol.TaskLease) error {
	q.mu.Lock()
	delete(q.leased, lease.Token)
	q.mu.Unlock()
	return nil
}

// Nack 把这个租约出队时的任务放回队尾；路由字段在任务上，重新投递沿用原来的路由。已确认、已退回或未知的
// 租约什么也不做。
func (q *TaskQueue) Nack(ctx context.Context, lease protocol.TaskLease) error {
	q.mu.Lock()
	task, ok := q.leased[lease.Token]
	delete(q.leased, lease.Token)
	q.mu.Unlock()
	if !ok {
		return nil
	}
	return q.Enqueue(ctx, task)
}

func engineKey(namespace, id string) string {
	return protocol.NormalizeNamespace(namespace) + "/" + id
}
