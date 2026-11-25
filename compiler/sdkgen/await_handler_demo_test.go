package sdkgen

import (
	"strings"
	"testing"
)

// 演示 + 锁定：sdkgen 能翻译"handler 内 workflow.Await 阻塞等主流程条件、结果同步回传"
// 这个 Temporal 惯用模式。用户直接写普通 Go（不用手工 WLang），go2wlang 翻译成
// handler 闭包内含 {"expr":{"Await":[...]}} + 主流程 ExecuteActivity。
// 运行：GOWORK=off go test ./sdkgen/ -run AwaitHandlerDemo -v
func TestAwaitHandlerDemo_HandlerInternalAwait_Translates(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

func CreateWorkflow(ctx workflow.Context, input map[string]any) error {
	var createDone bool
	var initResult map[string]any

	// update handler：阻塞到主流程把记录建好，再把结果同步回传给 POST 调用方
	workflow.SetUpdateHandler(ctx, "create", func(ctx workflow.Context, in map[string]any) (map[string]any, error) {
		_ = workflow.Await(ctx, func() bool { return createDone })
		return initResult, nil
	})

	// 主流程：建记录 → 置 createDone=true（唤醒 handler 的 Await）
	var r map[string]any
	_ = workflow.ExecuteActivity(ctx, activities.CreateRecord, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &r)
	initResult = r
	createDone = true
	return nil
}
`)
	result, err := GenerateFromSource(src, Options{WorkflowType: "create", FuncName: "CreateWorkflow", Filename: "create.go"})
	if err != nil {
		t.Fatalf("handler 内 Await 模式应能编译，却报错: %v", err)
	}
	prog := programText(t, result)
	// handler 闭包内含 Await 调用（不是被静默丢弃）。
	if !strings.Contains(prog, `"Await"`) {
		t.Errorf("编译产物应含 handler 内的 Await 调用，got: %s", prog)
	}
	// 主流程 activity（驱动条件的事件源）。
	if !strings.Contains(prog, "activities.CreateRecord") {
		t.Errorf("编译产物应含主流程 activity 调用")
	}
	t.Logf("✅ handler 内 Await 模式经 sdkgen 干净翻译（用户可直接写 Go，无需手工 WLang）")
}
