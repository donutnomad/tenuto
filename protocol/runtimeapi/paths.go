// Package runtimeapi 是 runtime HTTP API 的线上契约：各端点的路径、请求与应答类型、长轮询时长上界。
// 服务端 handler（server/http/runtimehttp）与客户端（sdk/internal/runtimeclient）共用本包，
// 本包只依赖 protocol。
package runtimeapi

import "time"

const (
	DefaultRuntimePollTimeout = 30 * time.Second
	// MaxRuntimePollTimeout 是单次长轮询在**服务端**被钳死的时长上界。
	//
	// 导出是为了让部署侧（HTTP server 的 WriteTimeout）能据此定值而不必抄一个字面量：
	// 两处若各写一份，改这里而忘了改那里就会静默掐断长轮询。
	MaxRuntimePollTimeout = time.Minute
)

const (
	WorkflowRuntimeSetWorkerTargetPath          = "/runtime/workflow/set-worker-target"
	WorkflowRuntimeRegisterPath                 = "/runtime/workflow/register"
	WorkflowRuntimeStartPath                    = "/runtime/workflow/start"
	WorkflowRuntimeStartVersionPath             = "/runtime/workflow/start-version"
	WorkflowRuntimeCancelPath                   = "/runtime/workflow/cancel"
	WorkflowRuntimeTerminatePath                = "/runtime/workflow/terminate"
	WorkflowRuntimeGetRunPath                   = "/runtime/workflow/get-run"
	WorkflowRuntimeWaitClosedPath               = "/runtime/workflow/wait-closed"
	WorkflowRuntimeDispatchPath                 = "/runtime/workflow/dispatch-pending"
	WorkflowRuntimeRegisterActivityWorkerPath   = "/runtime/workflow/register-activity-worker-version"
	WorkflowRuntimeHeartbeatActivityWorkerPath  = "/runtime/workflow/heartbeat-activity-worker-version"
	WorkflowRuntimeDeregisterActivityWorkerPath = "/runtime/workflow/deregister-activity-worker-version"
	WorkflowRuntimeListRunsPath                 = "/runtime/workflow/list"
	WorkflowRuntimeRunStatsPath                 = "/runtime/workflow/run-stats"
	WorkflowRuntimeSearchAttributesPath         = "/runtime/workflow/search-attributes"
	WorkflowRuntimeHistoryPath                  = "/runtime/workflow/history"
	WorkflowRuntimeHistoryPagePath              = "/runtime/workflow/history-page"
	WorkflowRuntimeSignalPath                   = "/runtime/workflow/signal"
	WorkflowRuntimeQueryPath                    = "/runtime/workflow/query"
	WorkflowRuntimeUpdatePath                   = "/runtime/workflow/update"
	WorkflowRuntimeUpdateWithStartPath          = "/runtime/workflow/update-with-start"
	WorkflowRuntimeCapabilitiesPath             = "/runtime/workflow/capabilities"
	WorkflowRuntimeSignalWithStartPath          = "/runtime/workflow/signal-with-start"
	WorkflowRuntimeGetConfigPath                = "/runtime/workflow/get-config"
	WorkflowRuntimeArtifactsPath                = "/runtime/workflow/artifacts"
	WorkflowRuntimeSetPolicyPath                = "/runtime/workflow/set-policy"
	WorkflowRuntimeListConfigsPath              = "/runtime/workflow/list-configs"
	WorkflowRuntimeListWorkersPath              = "/runtime/workflow/list-workers"
	WorkflowRuntimeCreateSchedulePath           = "/runtime/workflow/create-schedule"
	WorkflowRuntimeGetSchedulePath              = "/runtime/workflow/get-schedule"
	WorkflowRuntimeUpdateSchedulePath           = "/runtime/workflow/update-schedule"
	WorkflowRuntimeDeleteSchedulePath           = "/runtime/workflow/delete-schedule"
	WorkflowRuntimePauseSchedulePath            = "/runtime/workflow/pause-schedule"
	WorkflowRuntimeResumeSchedulePath           = "/runtime/workflow/resume-schedule"
	WorkflowRuntimeListSchedulesPath            = "/runtime/workflow/list-schedules"
	WorkflowRuntimeTriggerSchedulePath          = "/runtime/workflow/trigger-schedule"
	WorkflowRuntimeStartBatchPath               = "/runtime/workflow/start-batch"
	WorkflowRuntimeGetBatchPath                 = "/runtime/workflow/get-batch"
	WorkflowRuntimeListBatchesPath              = "/runtime/workflow/list-batches"
	WorkflowRuntimeCancelBatchPath              = "/runtime/workflow/cancel-batch"
	WorkflowRuntimeResetPath                    = "/runtime/workflow/reset"
	WorkflowRuntimeResetActivityPath            = "/runtime/workflow/reset-activity"
	WorkflowRuntimeListTaskQueuesPath           = "/runtime/workflow/list-task-queues"
	WorkflowRuntimeListNamespacesPath           = "/runtime/workflow/list-namespaces"
	WorkflowRuntimeActivityStatesPath           = "/runtime/workflow/activity-states"
)

const (
	WorkflowTaskRuntimePollPath               = "/runtime/workflow-task/poll"
	WorkflowTaskRuntimeAckPath                = "/runtime/workflow-task/ack"
	WorkflowTaskRuntimeNackPath               = "/runtime/workflow-task/nack"
	WorkflowTaskRuntimeConfigPath             = "/runtime/workflow-task/config"
	WorkflowTaskRuntimeEventsPath             = "/runtime/workflow-task/events"
	WorkflowTaskRuntimeStatusPath             = "/runtime/workflow-task/status"
	WorkflowTaskRuntimeCompletePath           = "/runtime/workflow-task/complete"
	WorkflowTaskRuntimeTimedOutPath           = "/runtime/workflow-task/timed-out"
	WorkflowTaskRuntimeFailedPath             = "/runtime/workflow-task/failed"
	WorkflowTaskRuntimeActivationPath         = "/runtime/workflow-task/activation"
	WorkflowTaskRuntimeCompleteActivationPath = "/runtime/workflow-task/complete-activation"
)

const (
	ActivityRuntimePollPath      = "/runtime/activity/poll"
	ActivityRuntimeAckPath       = "/runtime/activity/ack"
	ActivityRuntimeNackPath      = "/runtime/activity/nack"
	ActivityRuntimeBeginPath     = "/runtime/activity/begin"
	ActivityRuntimeHeartbeatPath = "/runtime/activity/heartbeat"
	ActivityRuntimeCancelPath    = "/runtime/activity/cancellation"
	ActivityRuntimeCompletePath  = "/runtime/activity/complete"
	ActivityRuntimeFailPath      = "/runtime/activity/fail"
	ActivityRuntimeReleasePath   = "/runtime/activity/release"
)

// nonIdempotentPaths 是重发会再执行一次的端点：Reset 产生新的 Run，触发 Schedule 启动新的 Run，
// 登记批量操作与创建 Schedule 各新建一条记录，请求里都没有去重键。
var nonIdempotentPaths = map[string]bool{
	WorkflowRuntimeResetPath:           true,
	WorkflowRuntimeCreateSchedulePath:  true,
	WorkflowRuntimeTriggerSchedulePath: true,
	WorkflowRuntimeStartBatchPath:      true,
}

// IsIdempotent 报告重发 path 上的请求是否不会重复生效。客户端只在请求确定没有被处理时重试非幂等端点；
// Server 在非幂等端点上不以 503 应答可能已经部分生效的失败。
func IsIdempotent(path string) bool {
	return !nonIdempotentPaths[path]
}

// longPollPaths 是整个请求都在等待事件发生的端点：领取 Task、等 Run 结束。它们的时长由等待决定，
// Server 的在途请求上限不计入它们，停机时它们立即结束。Update 与 update-with-start 先拿锁写库、再等结果，
// 只有等结果的阶段不计入在途上限。
var longPollPaths = map[string]bool{
	WorkflowTaskRuntimePollPath:   true,
	ActivityRuntimePollPath:       true,
	WorkflowRuntimeWaitClosedPath: true,
}

// IsLongPoll 报告 path 是否是长轮询端点。
func IsLongPoll(path string) bool {
	return longPollPaths[path]
}
