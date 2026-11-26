---
title: Go Client API
description: sdk/client 与 sdk/worker 的类型与方法签名
status: Stable
---

# Go Client API

<FeatureStatus status="Stable" />

用户 import 的 SDK 包是 `sdk/workflow`、`sdk/activity`、`sdk/apperror`、`sdk/registration`、`sdk/client` 与 `sdk/worker`。本页列出 `sdk/client` 与 `sdk/worker` 的签名；用法见 [Go Client](/sdk/client) 与 [运行 Worker](/develop/workers)。

## client.Dial

```go
func Dial(options Options) (Client, error)

type Options struct {
    HostPort   string              // "host:port" 或完整 URL，必填
    Namespace  string              // 空值为 "default"
    Identity   string              // 缺省 "<pid>@<hostname>"
    HTTPClient *http.Client        // nil 时 Dial 自建
    AuthToken  string              // 非空时每个请求带 Authorization: Bearer <AuthToken>
    TLS        *tls.Config         // Dial 自建客户端的 TLS（服务端 CA、mTLS 客户端证书），与 HTTPClient 互斥
    Telemetry  telemetry.Providers // 零值不加 trace 与指标
    Retry      RetryOptions        // 瞬时错误的重试，零值取缺省值
}

type RetryOptions struct {
    Disabled           bool          // true 时每个调用只发一次
    InitialInterval    time.Duration // 缺省 200ms
    MaximumInterval    time.Duration // 缺省 10s
    BackoffCoefficient float64       // 缺省 2
    MaximumElapsedTime time.Duration // ctx 没有 deadline 时的重试时长，缺省 1 分钟
}
```

哪些失败重试、哪些调用只在请求确定没有被处理时重试，见 [瞬时错误与重试](/sdk/client#瞬时错误与重试)。

## client.Client

```go
type Client interface {
    ExecuteWorkflow(ctx context.Context, options StartWorkflowOptions, workflowType string, args ...any) (WorkflowRun, error)
    GetWorkflow(ctx context.Context, workflowID, runID string) WorkflowRun
    SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error
    SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg any,
        options StartWorkflowOptions, workflowType string, workflowArgs ...any) (WorkflowRun, error)
    CancelWorkflow(ctx context.Context, workflowID, runID string) error
    TerminateWorkflow(ctx context.Context, workflowID, runID, reason string) error
    QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...any) (EncodedValue, error)
    UpdateWorkflow(ctx context.Context, options UpdateWorkflowOptions) (WorkflowUpdateHandle, error)
    UpdateWithStartWorkflow(ctx context.Context, options UpdateWithStartWorkflowOptions) (WorkflowUpdateHandle, error)
    NewWithStartWorkflowOperation(options StartWorkflowOptions, workflowType string, args ...any) WithStartWorkflowOperation
    DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*WorkflowExecutionDescription, error)
    GetWorkflowHistory(ctx context.Context, workflowID, runID string) HistoryEventIterator
    ListWorkflow(ctx context.Context, options ListWorkflowOptions) (*ListWorkflowResult, error)
    ResetWorkflowExecution(ctx context.Context, options ResetWorkflowExecutionOptions) (string, error)
    UpdateWorkflowExecutionOptions(ctx context.Context, options UpdateWorkflowExecutionOptions) error
    ScheduleClient() ScheduleClient
    StartBatchOperation(ctx context.Context, options StartBatchOperationOptions) (*BatchOperation, error)
    DescribeBatchOperation(ctx context.Context, operationID string) (*BatchOperation, error)
    ListBatchOperations(ctx context.Context, options ListBatchOperationsOptions) (*ListBatchOperationsResult, error)
    StopBatchOperation(ctx context.Context, operationID string) (*BatchOperation, error)
    Close()
}
```

`runID` 为空的方法作用于 Workflow ID 的当前 Run。

## 启动与结果

```go
type StartWorkflowOptions struct {
    ID                       string
    WorkflowIDConflictPolicy WorkflowIDConflictPolicy
}

const (
    WorkflowIDConflictPolicyUnspecified WorkflowIDConflictPolicy = iota
    WorkflowIDConflictPolicyFail        // 只用于 ExecuteWorkflow
    WorkflowIDConflictPolicyUseExisting // 只用于 UpdateWithStartWorkflow，且必须设置
)

type WorkflowRun interface {
    GetID() string
    GetRunID() string
    Get(ctx context.Context, valuePtr any) error
}

type EncodedValue interface {
    HasValue() bool
    Get(valuePtr any) error
}
```

`WorkflowRun.Get` 跟随 continue-as-new 与 Workflow 重试到链上最后一个 Run，见 [Get 跟随执行链](/sdk/client#get-跟随执行链)。

## Update

```go
type UpdateWorkflowOptions struct {
    UpdateID     string // 为空时 SDK 生成 UUID
    WorkflowID   string
    RunID        string
    UpdateName   string
    Args         []any
    WaitForStage WorkflowUpdateStage // Unspecified、Accepted、Completed
}

type WorkflowUpdateHandle interface {
    WorkflowID() string
    RunID() string
    UpdateID() string
    Get(ctx context.Context, valuePtr any) error
}

type UpdateWithStartWorkflowOptions struct {
    StartWorkflowOperation WithStartWorkflowOperation
    UpdateOptions          UpdateWorkflowOptions // WorkflowID 与 RunID 留空
}

type WithStartWorkflowOperation interface {
    Get(ctx context.Context) (WorkflowRun, error)
}
```

## 读取与管理

```go
type WorkflowExecutionDescription struct {
    WorkflowID, RunID, WorkflowType string
    Status                          WorkflowExecutionStatus
    FirstRunID, PreviousRunID       string
    ParentRunID                     string
    Attempt                         int
    StartTime                       time.Time
    ExecutionDeadline, RunDeadline  *time.Time
    SearchAttributes                map[string]string
    Policy                          workflow.Policy
    ProgramVersion                  string
    ConfigError, UpgradeError       string
    IsCurrent                       bool
    Blocked                         *WorkflowTaskBlocked
}

type HistoryEventIterator interface {
    HasNext() bool
    Next() (*history.Event, error)
}

type ListWorkflowOptions struct {
    WorkflowID       string
    Statuses         []WorkflowExecutionStatus
    SearchAttributes map[string]string
    Limit            int
    NewestFirst      bool
    SkipTotal        bool   // 不计算 Total（为 0）
    NextPageToken    string // 上一页的 ListWorkflowResult.NextPageToken；过滤条件变了再用返回 ErrInvalidArgument
}

type ListWorkflowResult struct {
    Executions    []WorkflowExecutionInfo
    Total         int
    NextPageToken string // 有下一页时非空
}

type ResetWorkflowExecutionOptions struct {
    WorkflowID, RunID   string
    Reason              string
    ResetType           ResetType // ResetTypeFirstWorkflowTask、ResetTypeLastWorkflowTask
    EventID             int64     // 与 ResetType 二选一
    ReapplyExcludeTypes []ResetReapplyExcludeType
}

type UpdateWorkflowExecutionOptions struct {
    WorkflowID, RunID string
    Policy            workflow.Policy // PolicyPinned、PolicyAutoUpgrade
}
```

`WorkflowExecutionStatus` 取 `Running`、`Completed`、`Failed`、`Canceled`、`Terminated`、`ContinuedAsNew`（常量前缀 `WorkflowExecutionStatus`）。

## Schedule

```go
type ScheduleClient interface {
    Create(ctx context.Context, options ScheduleOptions) (ScheduleHandle, error)
    GetHandle(ctx context.Context, scheduleID string) ScheduleHandle
    List(ctx context.Context, options ScheduleListOptions) (*ScheduleListResult, error)
}

type ScheduleHandle interface {
    GetID() string
    Describe(ctx context.Context) (*ScheduleDescription, error)
    Update(ctx context.Context, options ScheduleUpdateOptions) error
    Pause(ctx context.Context) error
    Unpause(ctx context.Context) error
    Trigger(ctx context.Context) (WorkflowRun, error)
    Delete(ctx context.Context) error
}

type ScheduleOptions struct {
    WorkflowType   string
    Args           []any
    CronExpression string
    TimeZone       string
    Description    string
    OverlapPolicy  ScheduleOverlapPolicy // ScheduleOverlapPolicySkip（缺省）、ScheduleOverlapPolicyAllowAll
    Paused         bool
    CatchupWindow  time.Duration // 0 取 365 天，最小 10 秒
}
```

`ScheduleDescription` 带 `CatchupWindow`、`MissedCatchupWindow`（超出窗口没有触发的到期点数）与 `BufferDropped`（待补到期点超过 1000 个时丢弃的数量）；`ScheduleUpdateOptions.CatchupWindow` 是 `*time.Duration`。

```go
```

## 批量操作

```go
type StartBatchOperationOptions struct {
    Type      BatchOperationType // cancel、terminate、reset
    Filter    ListWorkflowOptions
    Reason    string
    ResetType ResetType
}
```

`BatchOperation` 带 `ID`、`Type`、`State`（`pending`、`running`、`completed`、`failed`、`cancelled`）、各计数、`Errors` 样本与时间戳。

## 错误

```go
var (
    ErrInvalidArgument                 // 400
    ErrUnauthenticated                 // 401，不重试
    ErrNotFound                        // 404
    ErrConflict                        // 409
    ErrWorkflowExecutionAlreadyStarted // 启动类调用的 409，同时满足 ErrConflict
    ErrDeadlineExceeded                // 504
    ErrUnavailable                     // 503 或 502：Server 依赖的数据库或任务队列暂时不可用，重试用尽后返回
    ErrInternal                        // 500
    ErrUpdateRejected                  // Update 的 validator 拒绝
)

type ServiceError struct {
    StatusCode int
    Message    string
}

type WorkflowExecutionError struct {
    WorkflowID, RunID, WorkflowType string
}
func (e *WorkflowExecutionError) Unwrap() error

type TerminatedError struct{ Reason string }
type QueryFailedError struct{ QueryType, Message string }

const (
    TimeoutTypeWorkflowExecution
    TimeoutTypeWorkflowRun
)
```

## worker.New

```go
func New(c client.Client, options Options) (*Worker, error)

func (w *Worker) Start(ctx context.Context)
func (w *Worker) WaitReady(ctx context.Context) error
func (w *Worker) Run(ctx context.Context) error
func (w *Worker) Stop()

type Options struct {
    Role                                   Role // RoleBoth、RoleWorkflow、RoleActivity
    Workflows                              []registration.Definition
    Activities                             *activity.Registry
    ActivityTaskQueues                     []string
    WorkflowConcurrency                    int
    ActivityConcurrency                    int
    WorkflowTaskTimeout                    time.Duration
    Identity                               string
    BuildID                                string
    ActivityWorkerVersionDefault           bool
    ActivityWorkerVersionWeight            int
    ActivityWorkerVersionHeartbeatInterval time.Duration
    ActivityWorkerVersionHeartbeatTTL      time.Duration
    GracefulStopTimeout                    time.Duration
    ExecutionCache                         *ExecutionCacheOptions
    Telemetry                              telemetry.Providers
}

type ExecutionCacheOptions struct {
    MaxEntries        int
    MaxEstimatedBytes int64
    IdleTTL           time.Duration
}

const (
    DefaultWorkflowTaskTimeout = 10 * time.Second
    DefaultGracefulStopTimeout = 10 * time.Second
)
```
