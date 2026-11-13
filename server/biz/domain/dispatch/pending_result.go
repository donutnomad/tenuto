package dispatch

// WorkflowDispatchResult 统计一轮补投递发出的任务数。
type WorkflowDispatchResult struct {
	WorkflowTasks int `json:"workflow_tasks"`
	ActivityTasks int `json:"activity_tasks"`
}
