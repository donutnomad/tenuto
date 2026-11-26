// Package executionview 组装面向 console 的读模型：Run 详情与历史、namespace 统计、workflow 配置列表。
// 它只读取各领域的仓储接口；列表过滤、分页与历史分页规则由执行域提供。
package executionview

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
)

// RunReader 是 view 读取执行域用到的仓储方法，由 execution.ExecutionManager 的实现满足。
type RunReader interface {
	execution.HistoryPageStore
	execution.VisibilityStore
	Get(runID string) (*execution.Run, error)
	GetCurrentRunID(namespace, workflowID string) (string, error)
	CountRuns(namespace string) ([]execution.RunCount, error)
	GetEvents(runID string) ([]history.Event, error)
	GetWorkflowExecutionState(runID string) (*execution.WorkflowExecutionState, error)
	ListActivityStates(runID string) ([]*execution.ActivityState, error)
}

// WorkerVersions 提供活跃的 Activity Worker 版本，由 dispatch.WorkerVersionRepository 的实现满足。
type WorkerVersions interface {
	ListActiveActivityWorkerVersions(now time.Time) ([]protocol.ActivityWorkerVersion, error)
}

// Schedules 提供 namespace 内的 Schedule，由 schedule.Repository 的实现满足。
type Schedules interface {
	ListSchedules(namespace string) ([]*schedule.Schedule, error)
}

type View struct {
	runs      RunReader
	configs   definition.ConfigRepository
	workers   WorkerVersions
	schedules Schedules
	clock     clock.Clock
}

func New(runs RunReader, configs definition.ConfigRepository, workers WorkerVersions, schedules Schedules, clk clock.Clock) *View {
	return &View{runs: runs, configs: configs, workers: workers, schedules: schedules, clock: clk}
}
