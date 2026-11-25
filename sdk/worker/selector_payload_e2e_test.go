package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

// ============================================
// Selector 回调里读取的 Signal 改了 JSON 字段名
// ============================================
// 测试策略:
// - Error Case: Workflow 在 Selector 的 AddSignalFuture 回调里 Get approve Signal（名字是局部变量），新版本把
//   Approval 的 JSON 字段 order_id 改成 orderId。auto_upgrade Run 收到 approve 后停在 ship 上，新版本上线后换版前的
//   载荷检查发现 History 里 approve 的 order_id 在新类型里不存在：Run 留在旧 Program 以 ConfigError 暂停，没有
//   Workflow Task 在新版本上执行，不产出 order_id 为空的结果
// - Happy Path: 旧 build 回来后 set-policy pinned，Run 在旧 Program 上用正确的载荷完成
// ============================================

const selectorWorkflowSrc = `package %[1]s

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type Input struct {
	OrderID string ` + "`json:\"order_id\"`" + `
}

type Approval struct {
	OrderID  string ` + "`json:\"%[2]s\"`" + `
	Approver string ` + "`json:\"approver\"`" + `
}

type Result struct {
	OrderID  string ` + "`json:\"order_id\"`" + `
	Approver string ` + "`json:\"approver\"`" + `
}

// @Workflow(type="selector-order")
func Order(ctx workflow.Context, in Input) (Result, error) {
	var approval Approval
	name := "approve"
	approve := workflow.WaitSignal(ctx, name)
	selector := workflow.NewSelector(ctx)
	selector.AddSignalFuture(ctx, approve, func(f workflow.SignalFuture) {
		_ = f.Get(ctx, &approval)
	})
	selector.Select(ctx)
	var shipped map[string]any
	if err := workflow.WaitSignal(ctx, "ship").Get(ctx, &shipped); err != nil {
		return Result{}, err
	}
	return Result{OrderID: approval.OrderID, Approver: approval.Approver}, nil
}
`

func TestSelectorSignalFieldRenamePausesAutoUpgradeRun(t *testing.T) {
	v1 := generateSelectorWorkflow(t, "selector", "selector", "order_id")
	v2 := generateSelectorWorkflow(t, "camel/selector2", "selector2", "orderId")
	v1Version, err := program.WorkflowConfigVersion(v1)
	if err != nil {
		t.Fatal(err)
	}
	v2Version, err := program.WorkflowConfigVersion(v2)
	if err != nil {
		t.Fatal(err)
	}

	env := startRenameEnv(t)
	w1 := env.startWorker(t, "selector-w1", "b1", v1, renameCharge, nil)
	const id = "selector-auto"
	if _, err := env.c.ExecuteWorkflow(env.ctx, client.StartWorkflowOptions{ID: id}, "selector-order", map[string]any{"order_id": id}); err != nil {
		t.Fatal(err)
	}
	if err := env.c.SignalWorkflow(env.ctx, id, "", "approve", map[string]any{"order_id": id + "-approved", "approver": "alice"}); err != nil {
		t.Fatal(err)
	}
	env.waitSignalHandled(t, id, "approve")
	w1.Stop()

	env.startWorker(t, "selector-w2", "b2", v2, renameCharge, nil)
	if err := env.c.SignalWorkflow(env.ctx, id, "", "ship", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	description := env.waitConfigError(t, id)
	for _, want := range []string{
		"auto upgrade to " + v2Version + " refused",
		`signal "approve": selector.Approval -> selector2.Approval: order_id: field is missing in the new type (removed or renamed)`,
	} {
		if !strings.Contains(description.ConfigError, want) {
			t.Fatalf("config error = %q, want it to contain %q", description.ConfigError, want)
		}
	}
	if description.ProgramVersion != v1Version || description.Status != client.WorkflowExecutionStatusRunning {
		t.Fatalf("paused run: version %s status %s", description.ProgramVersion, description.Status)
	}
	for _, event := range env.history(t, id) {
		if started := event.WorkflowTaskStarted; started != nil && started.ProgramVersion != v1Version {
			t.Fatalf("workflow task %d ran %s", event.ID, started.ProgramVersion)
		}
	}

	env.startWorker(t, "selector-w1-back", "b1", v1, renameCharge, nil)
	if err := env.c.UpdateWorkflowExecutionOptions(env.ctx, client.UpdateWorkflowExecutionOptions{WorkflowID: id, Policy: workflow.PolicyPinned}); err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, env.result(t, id), map[string]any{"order_id": id + "-approved", "approver": "alice"})
	env.assertProgramVersion(t, id, workflow.PolicyPinned, v1Version)
}

// generateSelectorWorkflow 用真实生成器从 selectorWorkflowSrc 生成包 pkg（目录 dir），Approval 的第一个字段用 JSON
// 名 orderIDField。
func generateSelectorWorkflow(t *testing.T, dir, pkg, orderIDField string) *program.WorkflowConfig {
	t.Helper()
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("testdata", "tmp-selector-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(root)
		_ = os.Remove("testdata") // 只在目录已空时删除
	})
	dir = filepath.Join(root, dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "order.go"), fmt.Appendf(nil, selectorWorkflowSrc, pkg, orderIDField), 0o644); err != nil {
		t.Fatal(err)
	}
	plan, err := sdkgen.Scan(dir)
	if err != nil {
		t.Fatalf("scan %s: %v", pkg, err)
	}
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	result, err := sdkgen.Generate(plan)
	if err != nil {
		t.Fatalf("generate %s: %v", pkg, err)
	}
	return result.Workflow
}

// waitSignalHandled 等到 History 里有 signal，且最后一个事件是完成的 Workflow Task。
func (e *renameEnv) waitSignalHandled(t *testing.T, workflowID, signal string) {
	t.Helper()
	for {
		events := e.history(t, workflowID)
		signaled := false
		for _, event := range events {
			signaled = signaled || (event.WorkflowSignaled != nil && event.WorkflowSignaled.Name == signal)
		}
		if signaled && events[len(events)-1].WorkflowTaskCompleted != nil {
			return
		}
		select {
		case <-e.ctx.Done():
			t.Fatalf("%s did not handle %s: %d events", workflowID, signal, len(events))
		case <-time.After(20 * time.Millisecond):
		}
	}
}
