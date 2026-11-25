package sdkgen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRenderBridgeRegisterActivities 锁住包级批量注册 helper 的验收点：
// 本地实现全部注册且按 CallName 稳定排序，remote 契约只保留调用桥、绝不进入注册 helper。
func TestRenderBridgeRegisterActivities(t *testing.T) {
	contracts := []Contract{
		{Type: "orders.alpha", Version: "v1", Input: "AlphaInput", Output: "AlphaOutput", MarkerName: "Alpha", CallName: "CallAlpha"},
		{Type: "orders.beta", Queue: "orders", Version: "v2", Input: "BetaInput", Output: "BetaOutput", MarkerName: "Beta", CallName: "CallBeta"},
		{Type: "payments.settle", Version: "v1", Remote: true, Input: "SettleInput", Output: "SettleOutput", MarkerName: "SettleRemoteActivity", CallName: "CallSettle"},
	}
	content := string(renderBridge("orders", contracts))
	if content != string(renderBridge("orders", contracts)) {
		t.Fatal("同输入两次渲染必须逐字节相同")
	}
	for _, want := range []string{
		"func RegisterActivities(registry *activity.Registry) error {",
		`if err := activity.RegisterActivity(registry, "orders.alpha", "v1", Alpha); err != nil {`,
		// 带 queue 的契约连同 queue 一起登记，Worker 据此推导拉取的队列。
		`if err := activity.RegisterActivityOnQueue(registry, "orders", "orders.beta", "v2", Beta); err != nil {`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("注册 helper 缺少 %q\n%s", want, content)
		}
	}
	if strings.Index(content, `"orders.alpha", "v1", Alpha`) > strings.Index(content, `"orders.beta", "v2", Beta`) {
		t.Fatalf("注册顺序必须按 CallName 稳定排序\n%s", content)
	}
	helper := content[strings.Index(content, "func RegisterActivities"):]
	if strings.Contains(helper, "Settle") || strings.Contains(helper, "payments.settle") {
		t.Fatalf("remote 契约不能进入注册 helper\n%s", helper)
	}

	// remote-only 包不生成注册 helper：那里的实现由拥有该 activity 的 worker 注册。
	remoteOnly := string(renderBridge("payments", contracts[2:]))
	if strings.Contains(remoteOnly, "RegisterActivities") {
		t.Fatalf("remote-only 包不应生成注册 helper\n%s", remoteOnly)
	}
	if !strings.Contains(remoteOnly, "func CallSettle(ctx workflow.Context") {
		t.Fatalf("remote-only 包仍要生成调用桥\n%s", remoteOnly)
	}
}

// helper 把 activity.RegisterActivity 的错误（I/O 类型不受支持、activity type 重复登记）原样返回。
func TestRenderBridgeRegisterActivitiesErrorHandling(t *testing.T) {
	content := string(renderBridge("orders", []Contract{
		{Type: "orders.alpha", Version: "", Input: "AlphaInput", Output: "AlphaOutput", MarkerName: "Alpha", CallName: "CallAlpha"},
	}))
	want := "\tif err := activity.RegisterActivity(registry, \"orders.alpha\", \"\", Alpha); err != nil {\n\t\treturn err\n\t}\n"
	if !strings.Contains(content, want) {
		t.Fatalf("helper 必须返回 RegisterActivity 的错误\n%s", content)
	}
}

// TestScanContractsQueueBelongsToActivity 锁住队列归属：queue 只来自 @Activity 自己的注解，与同包的 @Workflow
// 无关。写了 queue 的连同队列登记；没写的不带队列登记，由 Worker 的 ActivityTaskQueues 决定。
func TestScanContractsQueueBelongsToActivity(t *testing.T) {
	dir := t.TempDir()
	src := `package orders

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type In struct {
	ID string ` + "`json:\"id\"`" + `
}

type Out struct {
	OK bool ` + "`json:\"ok\"`" + `
}

// @Activity(type="orders.notify",version=v1)
func Notify(_ context.Context, in In) (Out, error) { return Out{OK: true}, nil }

// @Activity(type="orders.audit",queue="audit",version=v1)
func Audit(_ context.Context, in In) (Out, error) { return Out{OK: true}, nil }

// @Workflow(type="place-order")
func Place(ctx workflow.Context, in In) (Out, error) { return CallNotify(ctx, in) }
`
	if err := os.WriteFile(filepath.Join(dir, "orders.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	contracts, _, err := scanContracts(dir, moduleInfo{})
	if err != nil {
		t.Fatal(err)
	}
	queues := map[string]string{}
	for _, contract := range contracts {
		queues[contract.Type] = contract.Queue
	}
	if queues["orders.notify"] != "" || queues["orders.audit"] != "audit" {
		t.Fatalf("queues = %v, want orders.notify without queue and orders.audit=audit", queues)
	}
	content := string(renderBridge("orders", contracts))
	for _, want := range []string{
		`activity.RegisterActivity(registry, "orders.notify", "v1", Notify)`,
		`activity.RegisterActivityOnQueue(registry, "audit", "orders.audit", "v1", Audit)`,
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("注册 helper 缺少 %q\n%s", want, content)
		}
	}
}
