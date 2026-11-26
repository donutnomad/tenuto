package executionrepo

import (
	"maps"
	"slices"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
)

// ListRecoveryRunIDs 实现 execution.Store 的周期补偿候选查询。每个条件一条走索引的查询，只取仍为 Running 的 Run，
// 结果在 Go 里合并去重。Activity 在 Run 结束的事务里转 run_closed（CloseOpenActivities），scheduled / started
// 的 Activity 只属于运行中的 Run，两条 Activity 查询不关联 runs；timer 与 execution state 按 run_id 关联 runs 过滤状态：
//
//	Run 截止时间          idx_workflow_runs_status_deadline / idx_workflow_runs_status_run_deadline
//	Scheduled timer       idx_timer_states_status (status, deadline)
//	Scheduled Activity    idx_activity_states_status (status, timeout_at) 的前缀
//	Started Activity 超时 idx_activity_states_status (status, timeout_at)
//	open WFT              idx_workflow_execution_states_open_wft
//	待调度 WFT            idx_workflow_execution_states_needs_wft
//	blocked               idx_workflow_execution_states_blocked
func (s *Repository) ListRecoveryRunIDs(dueBefore time.Time) ([]string, error) {
	running := string(history.StatusRunning)
	runs, timers, activities, states := RunSchema, TimerStateSchema, ActivityStateSchema, WorkflowExecutionStateSchema
	timersOfRunning := gsql.InnerJoin(&runs).On(runs.ID.EqF(timers.RunID)).And(runs.Status.Eq(running))
	statesOfRunning := gsql.InnerJoin(&runs).On(runs.ID.EqF(states.RunID)).And(runs.Status.Eq(running))
	queries := []*gsql.QueryBuilderG[string]{
		gsql.PluckG[string](runs.ID).From(runs).Where(runs.Status.Eq(running), runs.DeadlineAt.Lt(&dueBefore)),
		gsql.PluckG[string](runs.ID).From(runs).Where(runs.Status.Eq(running), runs.RunDeadlineAt.Lt(&dueBefore)),
		gsql.PluckG[string](timers.RunID).From(timers).Join(timersOfRunning).
			Where(timers.Status.Eq(string(execution.TimerStatusEnums.Scheduled)), timers.Deadline.Lt(dueBefore)),
		gsql.PluckG[string](activities.RunID).From(activities).
			Where(activities.Status.Eq(string(execution.ActivityStatusEnums.Scheduled))),
		gsql.PluckG[string](activities.RunID).From(activities).
			Where(activities.Status.Eq(string(execution.ActivityStatusEnums.Started)), activities.TimeoutAt.Lt(&dueBefore)),
		gsql.PluckG[string](states.RunID).From(states).Join(statesOfRunning).Where(states.OpenWorkflowTaskEventID.Gt(0)),
		gsql.PluckG[string](states.RunID).From(states).Join(statesOfRunning).Where(states.NeedsWorkflowTask.Eq(true)),
		gsql.PluckG[string](states.RunID).From(states).Join(statesOfRunning).Where(states.Blocked.Eq(true)),
	}
	candidates := map[string]struct{}{}
	for _, query := range queries {
		rows, err := infradb.Find(s.db, query.Distinct())
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			candidates[*row] = struct{}{}
		}
	}
	runIDs := slices.Sorted(maps.Keys(candidates))
	if len(runIDs) == 0 {
		return nil, nil
	}
	return runIDs, nil
}
