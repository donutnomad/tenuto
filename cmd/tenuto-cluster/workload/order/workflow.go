// Package order 是 tenuto-cluster failover / soak / upgrade 模式的 Workflow：顺序与并行 Activity、Timer、
// 带超时的 Signal 等待和并行的子 Workflow（cluster-steps），结果只由输入决定。Pad 是随每个 Activity
// 输入与输出往返的填充载荷，用来放大 History。
package order

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// Input 与 workload.OrderParams 同形。
type Input struct {
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

// Output 的 Total 与 Children 只由输入决定；Signal 是 "received" 或 "timeout"。
type Output struct {
	Total    int64         `json:"total"`
	Children []ChildOutput `json:"children"`
	Signal   string        `json:"signal"`
}

// ChildInput 与 steps.Input 同形。
type ChildInput struct {
	Key        string `json:"key"`
	Steps      int    `json:"steps"`
	TimerMs    int64  `json:"timer_ms"`
	ActivityMs int64  `json:"activity_ms"`
	Pad        string `json:"pad,omitempty"`
}

// ChildOutput 与 steps.Output 同形。
type ChildOutput struct {
	Total int64 `json:"total"`
	Steps int   `json:"steps"`
}

type ComputeInput struct {
	Key     string `json:"key"`
	Step    string `json:"step"`
	SleepMs int64  `json:"sleep_ms"`
	Pad     string `json:"pad,omitempty"`
}

// ComputeOutput 的 Pad 原样带回输入的 Pad。
type ComputeOutput struct {
	Value int64  `json:"value"`
	Pad   string `json:"pad,omitempty"`
}

// @Workflow(type="cluster-order",name="Cluster Order",version=v1)
func Order(ctx workflow.Context, in Input) (Output, error) {
	total := int64(0)
	for i := 0; i < in.Sequential; i++ {
		out, err := CallCompute(ctx, ComputeInput{Key: in.Key, Step: fmt.Sprintf("seq-%d", i), SleepMs: in.ActivityMs, Pad: in.Pad}, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         workflow.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaxInterval: 5 * time.Second},
		})
		if err != nil {
			return Output{}, err
		}
		total += out.Value
	}

	values := make([]int64, in.Parallel)
	failure := ""
	wg := workflow.NewWaitGroup(ctx)
	for i := 0; i < in.Parallel; i++ {
		wg.Go(ctx, func(ctx workflow.Context) {
			out, err := CallCompute(ctx, ComputeInput{Key: in.Key, Step: fmt.Sprintf("par-%d", i), SleepMs: in.ActivityMs, Pad: in.Pad}, workflow.ActivityOptions{
				StartToCloseTimeout: 10 * time.Second,
				RetryPolicy:         workflow.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaxInterval: 5 * time.Second},
			})
			if err != nil {
				failure = fmt.Sprintf("par-%d: %v", i, err)
				return
			}
			values[i] = out.Value
		})
	}
	wg.Wait(ctx)
	if failure != "" {
		return Output{}, workflow.NewApplicationError(failure, "ParallelStepFailed")
	}
	for _, value := range values {
		total += value
	}

	if err := workflow.Sleep(ctx, time.Duration(in.TimerMs)*time.Millisecond); err != nil {
		return Output{}, err
	}

	signaled := false
	timedOut := false
	workflow.Go(ctx, func(ctx workflow.Context) {
		var payload map[string]any
		_ = workflow.WaitSignal(ctx, "proceed").Get(ctx, &payload)
		signaled = true
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		_ = workflow.Sleep(ctx, time.Duration(in.SignalTimeoutMs)*time.Millisecond)
		timedOut = true
	})
	if err := workflow.Await(ctx, func() bool { return signaled || timedOut }); err != nil {
		return Output{}, err
	}
	signal := "timeout"
	if signaled {
		signal = "received"
	}

	children := make([]ChildOutput, in.Children)
	childFailure := ""
	childGroup := workflow.NewWaitGroup(ctx)
	for i := 0; i < in.Children; i++ {
		childGroup.Go(ctx, func(ctx workflow.Context) {
			var child ChildOutput
			if err := workflow.ExecuteChildWorkflow(ctx, "cluster-steps", ChildInput{
				Key: fmt.Sprintf("%s/child-%d", in.Key, i), Steps: in.ChildSteps, TimerMs: in.TimerMs, ActivityMs: in.ActivityMs, Pad: in.Pad,
			}).Get(ctx, &child); err != nil {
				childFailure = fmt.Sprintf("child-%d: %v", i, err)
				return
			}
			children[i] = child
		})
	}
	childGroup.Wait(ctx)
	if childFailure != "" {
		return Output{}, workflow.NewApplicationError(childFailure, "ChildFailed")
	}
	return Output{Total: total, Children: children, Signal: signal}, nil
}

// @Activity(type="cluster.compute",queue="cluster",version=v1)
func Compute(ctx context.Context, in ComputeInput) (ComputeOutput, error) {
	value, err := workload.RunStep(ctx, in.Key, in.Step, in.SleepMs)
	if err != nil {
		return ComputeOutput{}, err
	}
	return ComputeOutput{Value: value, Pad: in.Pad}, nil
}
