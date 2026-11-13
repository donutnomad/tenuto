// Package workload 是 tenuto-cluster 的负载：两个 Workflow（order、steps 子包）共用的 Activity 结果函数、
// 期望结果与 Activity 执行记录。
//
// Activity 的结果只由输入决定（StepValue），因此任何一次 Run 的正确结果都能在调用端算出来（ExpectedOrder、
// ExpectedSteps）。每次 Activity 执行在开始与结束时各向本 Worker 的 JSONL 文件追加一行（Recorder），
// 运行结束后由 tenuto-cluster 与 History 对照，统计重复执行。
package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/sdk/activity"
)

// Workflow Type、Signal 名与 Activity 队列。
const (
	OrderWorkflowType = "cluster-order"
	StepsWorkflowType = "cluster-steps"
	SignalName        = "proceed"
	TaskQueue         = "cluster"
)

// StepValue 是 Activity 对 (key, step) 的结果，取值 [0, 1000)。
func StepValue(key, step string) int64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key + "|" + step))
	return int64(h.Sum32() % 1000)
}

// OrderParams 是 cluster-order 的输入参数（order.Input 的同形结构，调用端用它计算期望结果）。
// PadBytes 是填充载荷的长度，不进输入；Pad 只在启动时按它填入（所有 Run 共用同一个字符串），
// 保存的参数里留空，内存不随 Run 数放大。
type OrderParams struct {
	PadBytes        int    `json:"-"`
	Key             string `json:"key"`
	Sequential      int    `json:"sequential"`
	Parallel        int    `json:"parallel"`
	Children        int    `json:"children"`
	ChildSteps      int    `json:"child_steps"`
	TimerMs         int64  `json:"timer_ms"`
	SignalTimeoutMs int64  `json:"signal_timeout_ms"`
	ActivityMs      int64  `json:"activity_ms"`
	Pad             string `json:"pad,omitempty"`
}

// StepsParams 是 cluster-steps 的输入参数（steps.Input 的同形结构）。
type StepsParams struct {
	Key        string `json:"key"`
	Steps      int    `json:"steps"`
	TimerMs    int64  `json:"timer_ms"`
	ActivityMs int64  `json:"activity_ms"`
	Pad        string `json:"pad,omitempty"`
}

// ChildKey 是 cluster-order 交给第 i 个子 Workflow 的 key。
func ChildKey(key string, i int) string { return fmt.Sprintf("%s/child-%d", key, i) }

// Pad 返回 n 字节的填充载荷。
func Pad(n int) string { return strings.Repeat("p", n) }

// ExpectedOrderTotal 是 cluster-order 输出的 total：顺序与并行 Activity 结果之和。
func ExpectedOrderTotal(in OrderParams) int64 {
	var total int64
	for i := range in.Sequential {
		total += StepValue(in.Key, fmt.Sprintf("seq-%d", i))
	}
	for i := range in.Parallel {
		total += StepValue(in.Key, fmt.Sprintf("par-%d", i))
	}
	return total
}

// ExpectedStepsTotal 是 cluster-steps 输出的 total。
func ExpectedStepsTotal(key string, steps int) int64 {
	var total int64
	for i := range steps {
		total += StepValue(key, fmt.Sprintf("step-%d", i))
	}
	return total
}

// OrderActivityCount 是一次 cluster-order Run（不含子 Workflow）调度的 Activity 个数。
func OrderActivityCount(in OrderParams) int { return in.Sequential + in.Parallel }

// Execution 是 JSONL 里的一行：一次 Activity attempt 的开始（Phase=start）或结束（Phase=end）。
type Execution struct {
	Phase        string    `json:"phase"`
	Worker       string    `json:"worker"`
	WorkflowID   string    `json:"workflow_id"`
	RunID        string    `json:"run_id"`
	ActivityID   string    `json:"activity_id"`
	ActivityType string    `json:"activity_type"`
	Attempt      int       `json:"attempt"`
	Time         time.Time `json:"time"`
}

// Recorder 把 Execution 追加到一个文件。每行一次 write 调用，进程被 kill -9 时最多留下最后一行不完整。
type Recorder struct {
	worker string
	mu     sync.Mutex
	file   *os.File
}

// OpenRecorder 以追加方式打开 path；Worker 重启后继续写同一个文件。
func OpenRecorder(path, worker string) (*Recorder, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Recorder{worker: worker, file: file}, nil
}

// Close 关闭文件。
func (r *Recorder) Close() error { return r.file.Close() }

func (r *Recorder) write(ctx context.Context, phase string) {
	info := activity.GetInfo(ctx)
	line, err := json.Marshal(Execution{
		Phase: phase, Worker: r.worker, WorkflowID: info.WorkflowID, RunID: info.RunID,
		ActivityID: info.ActivityID, ActivityType: info.ActivityType, Attempt: info.Attempt, Time: time.Now(),
	})
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _ = r.file.Write(append(line, '\n'))
}

var (
	recorderMu sync.RWMutex
	recorder   *Recorder
)

// SetRecorder 设置本进程 Activity 写入的 Recorder；nil 表示不记录。
func SetRecorder(r *Recorder) {
	recorderMu.Lock()
	defer recorderMu.Unlock()
	recorder = r
}

// RunStep 是两个包的 Activity 共用的实现：记录开始，等待 sleepMs，记录结束，返回 StepValue。
// ctx 被取消时（Worker 停止的宽限期到期）返回 ctx 的错误，不记录结束。
func RunStep(ctx context.Context, key, step string, sleepMs int64) (int64, error) {
	recorderMu.RLock()
	r := recorder
	recorderMu.RUnlock()
	if r != nil {
		r.write(ctx, "start")
	}
	if sleepMs > 0 {
		timer := time.NewTimer(time.Duration(sleepMs) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
	if r != nil {
		r.write(ctx, "end")
	}
	return StepValue(key, step), nil
}
