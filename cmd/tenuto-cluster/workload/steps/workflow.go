// Package steps 是 tenuto-cluster 的轻量 Workflow：Steps 个顺序 Activity 加一个 Timer。cluster-order 把它作为
// 子 Workflow，bench 模式直接启动它。Pad 随每个 Activity 输入与输出往返。
package steps

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/cmd/tenuto-cluster/workload"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// Input 与 workload.StepsParams 同形。TimerMs 为 0 时不设 Timer。
type Input struct {
	Key        string `json:"key"`
	Steps      int    `json:"steps"`
	TimerMs    int64  `json:"timer_ms"`
	ActivityMs int64  `json:"activity_ms"`
	Pad        string `json:"pad,omitempty"`
}

type Output struct {
	Total int64 `json:"total"`
	Steps int   `json:"steps"`
}

type StepInput struct {
	Key     string `json:"key"`
	Step    string `json:"step"`
	SleepMs int64  `json:"sleep_ms"`
	Pad     string `json:"pad,omitempty"`
}

// StepOutput 的 Pad 原样带回输入的 Pad。
type StepOutput struct {
	Value int64  `json:"value"`
	Pad   string `json:"pad,omitempty"`
}

// @Workflow(type="cluster-steps",name="Cluster Steps",version=v1)
func Steps(ctx workflow.Context, in Input) (Output, error) {
	total := int64(0)
	for i := 0; i < in.Steps; i++ {
		out, err := CallStep(ctx, StepInput{Key: in.Key, Step: fmt.Sprintf("step-%d", i), SleepMs: in.ActivityMs, Pad: in.Pad}, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
			RetryPolicy:         workflow.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaxInterval: 5 * time.Second},
		})
		if err != nil {
			return Output{}, err
		}
		total += out.Value
	}
	if in.TimerMs > 0 {
		if err := workflow.Sleep(ctx, time.Duration(in.TimerMs)*time.Millisecond); err != nil {
			return Output{}, err
		}
	}
	return Output{Total: total, Steps: in.Steps}, nil
}

// @Activity(type="cluster.step",queue="cluster")
func Step(ctx context.Context, in StepInput) (StepOutput, error) {
	value, err := workload.RunStep(ctx, in.Key, in.Step, in.SleepMs)
	if err != nil {
		return StepOutput{}, err
	}
	return StepOutput{Value: value, Pad: in.Pad}, nil
}
