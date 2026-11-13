package dispatchrepo

import (
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Repository 是派发聚合的 gorm 仓储：任务派发租约（tenuto_task_dispatches）与
// Activity Worker 路由登记（tenuto_activity_worker_versions）。
type Repository struct {
	db infradb.DB
}

var (
	_ dispatch.Repository              = (*Repository)(nil)
	_ dispatch.WorkerVersionRepository = (*Repository)(nil)
)

func New(database infradb.DB) *Repository {
	return &Repository{db: database}
}

// AcquireTaskDispatch uses a conditional upsert to acquire a task lease atomically.
// MySQL evaluates assignments in order, so lease_expires_at and status are assigned
// last to preserve the values used by the acquisition condition.
func (s *Repository) AcquireTaskDispatch(task protocol.Task, leaseTTL time.Duration) (dispatch.Lease, bool, error) {
	task = dispatch.NormalizeTask(task)
	if task.Type == "" || task.RunID == "" || task.ScheduledEventID == 0 {
		return dispatch.Lease{}, false, nil
	}
	now := time.Now()
	leaseID := uuid.NewString()
	expiresAt := now.Add(leaseTTL)

	canPreempt := "tenuto_task_dispatches.status <> ? AND " +
		"NOT (tenuto_task_dispatches.status = ? AND tenuto_task_dispatches.lease_expires_at > ?)"
	preemptArgs := []any{string(dispatch.StatusDispatched), string(dispatch.StatusLeased), now}

	assign := func(col string, val any) clause.Assignment {
		args := append(append([]any{}, preemptArgs...), val)
		return clause.Assignment{
			Column: clause.Column{Name: col},
			Value:  gorm.Expr("IF("+canPreempt+", ?, `"+col+"`)", args...),
		}
	}

	row := taskDispatchRowFromTask(task, now)
	row.Status = string(dispatch.StatusLeased)
	row.LeaseID = leaseID
	row.LeaseExpiresAt = &expiresAt
	row.Attempts = 1

	// 撞主键 (namespace, task_type, run_id, scheduled_event_id, attempt) 时由上面的 IF 条件决定是否抢占
	// （ON DUPLICATE KEY UPDATE）。Columns 写明冲突键；MySQL 按表上的主键与唯一键判定冲突。
	err := s.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "namespace"}, {Name: "task_type"}, {Name: "run_id"},
			{Name: "scheduled_event_id"}, {Name: "attempt"},
		},

		DoUpdates: clause.Set{
			assign("activity_id", task.ActivityID),
			assign("activity_type", task.ActivityType),
			assign("task_queue", task.TaskQueue),
			assign("lease_id", leaseID),
			assign("updated_at", now),
			clause.Assignment{
				Column: clause.Column{Name: "attempts"},
				Value: gorm.Expr("IF("+canPreempt+", `attempts` + 1, `attempts`)",
					preemptArgs...),
			},

			assign("lease_expires_at", expiresAt),
			assign("status", string(dispatch.StatusLeased)),
		},
	}).Create(row).Error
	if err != nil {
		return dispatch.Lease{}, false, err
	}

	// 回读确认这一轮的赢家是不是自己：lease_id 是本次新生成的 UUID，
	// 读到它等价于"上面那条语句真的把租约写到了本调用名下"。
	storedRows, err := taskDispatchQuery(task).Find(s.db)
	if err != nil {
		return dispatch.Lease{}, false, err
	}
	if len(storedRows) == 0 || storedRows[0].LeaseID != leaseID {
		return dispatch.Lease{}, false, nil
	}
	return dispatch.Lease{Task: task, LeaseID: leaseID}, true, nil
}

func (s *Repository) MarkTaskDispatchDispatched(task protocol.Task, leaseID string) error {
	task = dispatch.NormalizeTask(task)
	now := time.Now()
	return taskDispatchQuery(task).Where(TaskDispatchSchema.LeaseID.Eq(leaseID)).Update(s.db, map[string]any{
		"status":             string(dispatch.StatusDispatched),
		"last_error":         "",
		"last_dispatched_at": &now,
		"updated_at":         now,
	}).Error
}

func (s *Repository) MarkTaskDispatchFailed(task protocol.Task, leaseID, errMsg string) error {
	task = dispatch.NormalizeTask(task)
	now := time.Now()
	return taskDispatchQuery(task).Where(TaskDispatchSchema.LeaseID.Eq(leaseID)).Update(s.db, map[string]any{
		"status":     string(dispatch.StatusFailed),
		"last_error": errMsg,
		"updated_at": now,
	}).Error
}

// ReleaseTaskDispatch 释放该 task 的 dispatch 记录，使其可被重新 Acquire。
//
// 守卫是必需的：调用方都是"caretaker/兜底清理"语义（崩溃恢复、reset 重派、sticky 超时回落），
// 不持有 leaseID。若无条件改写，B 实例的一次兜底清理会把 A 实例正在持有的未过期活跃租约
// 清成 failed，该 task 立刻可被重新 Acquire → 同一 activity 被两个 worker 并发执行。
// task token 的 stamp 只能在完成回调时拒绝陈旧结果，副作用（扣款/发消息）已经跑了两次。
//
// 故这里只清"非活跃"的行（OR 任一命中即可清）：
//   - lease_id = ”            —— 本就没有活跃租约；
//   - status <> 'leased'       —— dispatched/failed 都不是"活跃租约"；
//   - lease_expires_at IS NULL 或 <= now —— 租约已过期，清理是安全的。
//
// 被保护的是"status='leased' 且 lease_expires_at 仍在未来"的行——这恰好是 AcquireTaskDispatch
// 拒绝抢占的行集合（见其 canPreempt 条件），两者互补对齐。
//
// 匹配 0 行不是错误：说明有他人持活跃租约，那一方自己会完成投递，task 不会孤立。
func (s *Repository) ReleaseTaskDispatch(task protocol.Task, errMsg string) error {
	task = dispatch.NormalizeTask(task)
	now := time.Now()
	return taskDispatchQuery(task).
		Where(gsql.Or(
			TaskDispatchSchema.LeaseID.Eq(""),
			TaskDispatchSchema.Status.Not(string(dispatch.StatusLeased)),
			TaskDispatchSchema.LeaseExpiresAt.IsNull(),
			TaskDispatchSchema.LeaseExpiresAt.Lte(&now),
		)).
		Update(s.db, map[string]any{
			"status":           string(dispatch.StatusFailed),
			"lease_id":         "",
			"lease_expires_at": nil,
			"last_error":       errMsg,
			"updated_at":       now,
		}).Error
}

// GetTaskDispatch 读取 task 的派发记录。
func (s *Repository) GetTaskDispatch(task protocol.Task) (dispatch.State, bool, error) {
	task = dispatch.NormalizeTask(task)
	rows, err := taskDispatchQuery(task).Find(s.db)
	if err != nil {
		return dispatch.State{}, false, err
	}
	if len(rows) == 0 {
		return dispatch.State{}, false, nil
	}
	return taskDispatchRowToState(rows[0]), true, nil
}

// DeleteRunDispatches 在调用方的事务 tx 内删除这些 Run 的全部派发记录，供保留期回收整树删除时调用。
func DeleteRunDispatches(tx *gorm.DB, runIDs []string) error {
	if len(runIDs) == 0 {
		return nil
	}
	return gsql.From[TaskDispatchPO](TaskDispatchSchema).Where(TaskDispatchSchema.RunID.IN(runIDs...)).Delete(tx).Error
}

func taskDispatchQuery(task protocol.Task) *gsql.QueryBuilderG[TaskDispatchPO] {
	task = dispatch.NormalizeTask(task)
	return gsql.From[TaskDispatchPO](TaskDispatchSchema).Where(
		TaskDispatchSchema.Namespace.Eq(task.Namespace),
		TaskDispatchSchema.TaskType.Eq(string(task.Type)),
		TaskDispatchSchema.RunID.Eq(task.RunID),
		TaskDispatchSchema.ScheduledEventID.Eq(task.ScheduledEventID),
		TaskDispatchSchema.Attempt.Eq(task.Attempt),
	)
}
