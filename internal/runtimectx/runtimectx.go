// Package runtimectx 是 worker 运行时往上下文里写入、SDK 从上下文里读出的数据。
//
// 写入方（WithXxx）只给本模块的 worker 运行时用；作者在 sdk/activity 里只能拿到读取方。
// 放在 internal 下，模块外的代码无法导入。
package runtimectx

import (
	"context"
	"time"
)

// ActivityInfo 是一次 Activity attempt 的信息，字段含义与 Temporal 的 activity.Info 相同。
type ActivityInfo struct {
	// Namespace、WorkflowID、WorkflowType、RunID 标识调度这个 Activity 的 Workflow Run。
	Namespace    string
	WorkflowID   string
	WorkflowType string
	RunID        string
	// ActivityID 标识 Run 里被调度的这一个 Activity，同一个 Activity 的各次重试相同。
	ActivityID   string
	ActivityType string
	TaskQueue    string
	// Attempt 从 1 开始，每次重试加 1。
	Attempt int
	// ScheduledTime 是 Activity 首次被调度的时间，StartedTime 是本次 attempt 开始的时间。
	ScheduledTime time.Time
	StartedTime   time.Time
	// Deadline 是本次 attempt 必须结束的时间：StartToCloseTimeout 与 ScheduleToCloseTimeout 中先到的
	// 一个；两者都未设置时为零值。
	Deadline         time.Time
	HeartbeatTimeout time.Duration
	// TaskToken 是异步完成时交给外部系统的凭据。
	TaskToken string
}

type (
	activityInfoKey  struct{}
	heartbeatKey     struct{}
	lastHeartbeatKey struct{}
	workerStopKey    struct{}
)

// WithWorkerStop 写入 Worker 开始停止时关闭的 channel。
func WithWorkerStop(ctx context.Context, stop <-chan struct{}) context.Context {
	return context.WithValue(ctx, workerStopKey{}, stop)
}

func WorkerStopFrom(ctx context.Context) <-chan struct{} {
	stop, _ := ctx.Value(workerStopKey{}).(<-chan struct{})
	return stop
}

func WithActivityInfo(ctx context.Context, info ActivityInfo) context.Context {
	return context.WithValue(ctx, activityInfoKey{}, info)
}

func ActivityInfoFrom(ctx context.Context) ActivityInfo {
	info, _ := ctx.Value(activityInfoKey{}).(ActivityInfo)
	return info
}

// WithHeartbeat 写入上报心跳的函数。
func WithHeartbeat(ctx context.Context, record func(details map[string]any)) context.Context {
	return context.WithValue(ctx, heartbeatKey{}, record)
}

func HeartbeatFrom(ctx context.Context) func(details map[string]any) {
	record, _ := ctx.Value(heartbeatKey{}).(func(details map[string]any))
	return record
}

// WithLastHeartbeat 写入上一次尝试最后记录的心跳详情。
func WithLastHeartbeat(ctx context.Context, details map[string]any) context.Context {
	return context.WithValue(ctx, lastHeartbeatKey{}, details)
}

func LastHeartbeatFrom(ctx context.Context) map[string]any {
	details, _ := ctx.Value(lastHeartbeatKey{}).(map[string]any)
	return details
}
