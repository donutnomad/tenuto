package executionrepo

import (
	"maps"
	"slices"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

// ListRecoveryRunIDs 实现 execution.Store 的周期补偿候选查询。每个条件一条走索引的查询，结果在 Go 里合并去重，
// 最后只保留仍为 Running 的 Run：
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
	queries := []*gsql.QueryBuilderG[string]{
		gsql.PluckG[string](runs.ID).From(runs).Where(runs.Status.Eq(running), runs.DeadlineAt.Lt(&dueBefore)),
		gsql.PluckG[string](runs.ID).From(runs).Where(runs.Status.Eq(running), runs.RunDeadlineAt.Lt(&dueBefore)),
		gsql.PluckG[string](timers.RunID).From(timers).
			Where(timers.Status.Eq(string(execution.TimerStatusEnums.Scheduled)), timers.Deadline.Lt(dueBefore)),
		gsql.PluckG[string](activities.RunID).From(activities).
			Where(activities.Status.Eq(string(execution.ActivityStatusEnums.Scheduled))),
		gsql.PluckG[string](activities.RunID).From(activities).
			Where(activities.Status.Eq(string(execution.ActivityStatusEnums.Started)), activities.TimeoutAt.Lt(&dueBefore)),
		gsql.PluckG[string](states.RunID).From(states).Where(states.OpenWorkflowTaskEventID.Gt(0)),
		gsql.PluckG[string](states.RunID).From(states).Where(states.NeedsWorkflowTask.Eq(true)),
		gsql.PluckG[string](states.RunID).From(states).Where(states.Blocked.Eq(true)),
	}
	candidates := map[string]struct{}{}
	for _, query := range queries {
		rows, err := query.Distinct().Find(s.db)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			candidates[*row] = struct{}{}
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	rows, err := gsql.PluckG[string](runs.ID).From(runs).
		Where(runs.ID.In(slices.Collect(maps.Keys(candidates))...), runs.Status.Eq(running)).
		Find(s.db)
	if err != nil {
		return nil, err
	}
	runIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		runIDs = append(runIDs, *row)
	}
	slices.Sort(runIDs)
	return runIDs, nil
}
