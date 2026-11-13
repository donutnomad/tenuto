package sdkgen

import (
	"strings"
	"testing"
)

// TestRenderBridgeRegisterActivities 锁住包级批量注册 helper 的验收点：
// 本地实现全部注册且按 CallName 稳定排序，remote 契约只保留调用桥、绝不进入注册 helper。
func TestRenderBridgeRegisterActivities(t *testing.T) {
	contracts := []Contract{
		{Type: "orders.alpha", Version: "v1", Input: "AlphaInput", Output: "AlphaOutput", MarkerName: "Alpha", CallName: "CallAlpha"},
		{Type: "orders.beta", Version: "v2", Input: "BetaInput", Output: "BetaOutput", MarkerName: "Beta", CallName: "CallBeta"},
		{Type: "payments.settle", Version: "v1", Remote: true, Input: "SettleInput", Output: "SettleOutput", MarkerName: "SettleRemoteActivity", CallName: "CallSettle"},
	}
	content := string(renderBridge("orders", contracts))
	if content != string(renderBridge("orders", contracts)) {
		t.Fatal("同输入两次渲染必须逐字节相同")
	}
	for _, want := range []string{
		"func RegisterActivities(registry *activity.Registry) error {",
		`if err := activity.RegisterActivity(registry, "orders.alpha", "v1", Alpha); err != nil {`,
		`if err := activity.RegisterActivity(registry, "orders.beta", "v2", Beta); err != nil {`,
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
