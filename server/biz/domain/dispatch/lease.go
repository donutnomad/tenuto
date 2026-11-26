package dispatch

import (
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

type Status string

const (
	StatusLeased     Status = "leased"
	StatusDispatched Status = "dispatched"
	StatusFailed     Status = "failed"
)

// State 是一条派发记录。记录按 namespace、任务类型、Run、scheduled event 与 attempt 去重，
// 同一 attempt 的不同 stamp 落在同一条记录上。
type State struct {
	Namespace        string
	Status           Status
	LeaseID          string
	LeaseExpiresAt   time.Time
	Attempts         int
	LastError        string
	LastDispatchedAt time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// MaxRedeliveryWait 是重投等待时长的上限：等待从 base 起按派发次数翻倍，到 1 小时为止。生产 grace=30s 时
// 约 7 次翻倍后到达上限，Worker 下线 8 小时一个待领取任务约多出 13 份投递，之后每小时最多多一份。
const MaxRedeliveryWait = time.Hour

// RedeliveryDue 报告一条已投递（StatusDispatched）、任务却一直没有被领取的记录是否该重新投递。
//
// 已投递的记录挡住重复派发，它所依赖的前提是队列里的那份投递最终会被领取；投递在队列里丢失（队列故障，或
// 一份投递被错误地确认），记录就永远挡住补投。调用方只对仍待领取的任务（open 且未 started 的 Workflow Task、
// Scheduled 的 Activity attempt）询问。上一次投递距 now 超过等待时长时到期，等待时长从 base 起按派发次数
// 翻倍，最长 MaxRedeliveryWait：任务只是在等空闲 Worker 时，多出的投递份数随等待时长对数增长；多出的投递
// 在领取时按 state 丢弃。
func (s State) RedeliveryDue(now time.Time, base time.Duration) bool {
	if s.Status != StatusDispatched || s.LastDispatchedAt.IsZero() {
		return false
	}
	return s.LastDispatchedAt.Add(redeliveryWait(base, s.Attempts)).Before(now)
}

// redeliveryWait 是第 attempts 次派发之后的等待：base·2^(attempts-1)，不超过 MaxRedeliveryWait。
func redeliveryWait(base time.Duration, attempts int) time.Duration {
	wait := max(base, 0)
	for i := 1; i < attempts && wait < MaxRedeliveryWait; i++ {
		wait *= 2
	}
	return min(wait, MaxRedeliveryWait)
}

type Lease struct {
	Task    protocol.Task
	LeaseID string
}

// NormalizeTask 把任务整理成派发记录使用的形态：namespace 取默认值；
// Activity 任务的 attempt 从 1 起、task queue 取默认值；Workflow 任务的 attempt 固定为 0。
func NormalizeTask(task protocol.Task) protocol.Task {
	task.Namespace = protocol.NormalizeNamespace(task.Namespace)
	if task.Type == protocol.TaskTypeActivity {
		task.Attempt = max(task.Attempt, 1)
		task.TaskQueue = NormalizeActivityTaskQueue(task.TaskQueue)
		return task
	}
	task.Attempt = 0
	return task
}
