// Package sdkgentest 用 sdkgen 从 Go 源码生成测试用的 Workflow 配置。
package sdkgentest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
)

// Step 是 Sequential 里的一次 Activity 调用。Input 与 Options 是 Go 源码片段：
// Input 缺省为 Workflow 入参 input，可以引用 input 与前面步骤的结果 out0、out1…；
// Options 是 workflow.ActivityOptions 的字段，缺省为 StartToCloseTimeout: time.Minute。
type Step struct {
	Type    string
	Input   string
	Options string
}

// Sequential 用 sdkgen 从 Go 源码生成依次执行 steps 的程序，填进 base 后返回 base：任一 Activity
// 失败时 Workflow 返回该错误，全部成功时返回 nil。第 i 个 Activity 的 ActivityID 是 "#a<i+1>"。
func Sequential(t testing.TB, base *program.WorkflowConfig, steps ...Step) *program.WorkflowConfig {
	t.Helper()
	var body strings.Builder
	for i, step := range steps {
		input := step.Input
		if input == "" {
			input = "input"
		}
		options := step.Options
		if options == "" {
			options = "StartToCloseTimeout: time.Minute"
		}
		fmt.Fprintf(&body, `	var out%[1]d map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.A%[1]d, %[2]s, workflow.ActivityOptions{%[3]s}).Get(ctx, &out%[1]d); err != nil {
		return err
	}
`, i, input, options)
	}
	source := fmt.Sprintf(`package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
%s	return nil
}
`, body.String())
	generated, err := sdkgen.GenerateFromSource([]byte(source), sdkgen.Options{WorkflowType: base.Type, FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("generate %s: %v\n%s", base.Type, err, source)
	}
	for i, step := range steps {
		symbol := fmt.Sprintf("activities.A%d", i)
		ref := generated.Workflow.Activities[symbol]
		ref.Type = step.Type
		generated.Workflow.Activities[symbol] = ref
	}
	base.Program = generated.Workflow.Program
	base.Activities = generated.Workflow.Activities
	return base
}

// Loop 生成顺序调用 count 次 activityType 的 Workflow，填进 base 后返回 base。第 i 次调用的
// ActivityID 是 "#a<i+1>"。
func Loop(t testing.TB, base *program.WorkflowConfig, activityType string, count int) *program.WorkflowConfig {
	t.Helper()
	source := fmt.Sprintf(`package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func W(ctx workflow.Context, input map[string]any) error {
	for i := 0; i < %d; i++ {
		var out map[string]any
		if err := workflow.ExecuteActivity(ctx, activities.A0, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
			return err
		}
	}
	return nil
}
`, count)
	generated, err := sdkgen.GenerateFromSource([]byte(source), sdkgen.Options{WorkflowType: base.Type, FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatalf("generate %s: %v", base.Type, err)
	}
	ref := generated.Workflow.Activities["activities.A0"]
	ref.Type = activityType
	generated.Workflow.Activities["activities.A0"] = ref
	base.Program = generated.Workflow.Program
	base.Activities = generated.Workflow.Activities
	return base
}
