package sdkgen

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	programpkg "github.com/donutnomad/tenuto/program"
)

// fixtureDir 返回 testdata/orders 的绝对路径。
func fixtureDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("testdata", "orders"))
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// copyFixture 把 fixture 复制到 testdata 下的临时目录，供需要写生成文件的用例使用。
// 复制目标留在模块内，模块解析与 packages.Load 才能正常工作。
func copyFixture(t *testing.T) string {
	t.Helper()
	source := fixtureDir(t)
	module, err := findModule(source)
	if err != nil {
		t.Fatal(err)
	}
	target, err := os.MkdirTemp(filepath.Join("testdata"), "tmp-orders-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(target) })
	sourcePath, ok := module.importForDir(source)
	if !ok {
		t.Fatalf("%s 不在模块内", source)
	}
	targetPath, ok := module.importForDir(target)
	if !ok {
		t.Fatalf("%s 不在模块内", target)
	}
	if err := copyTree(source, target, sourcePath, targetPath); err != nil {
		t.Fatal(err)
	}
	return target
}

// copyTree 递归复制目录里的 Go 文件，并把 import 前缀改到副本上，
// 这样副本编译/生成都不依赖原 fixture（也不会往原目录写桥文件）。
func copyTree(source, target, sourcePath, targetPath string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		from := filepath.Join(source, entry.Name())
		to := filepath.Join(target, entry.Name())
		if entry.IsDir() {
			if err := os.MkdirAll(to, 0o755); err != nil {
				return err
			}
			if err := copyTree(from, to, sourcePath, targetPath); err != nil {
				return err
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasPrefix(entry.Name(), "zz_") {
			continue
		}
		data, err := os.ReadFile(from)
		if err != nil {
			return err
		}
		rewritten := strings.ReplaceAll(string(data), sourcePath, targetPath)
		if err := os.WriteFile(to, []byte(rewritten), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// writeBridges 把 plan 里的桥文件落盘（CLI 在 Generate 之前做的事）。
func writeBridges(t *testing.T, plan Plan) {
	t.Helper()
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatalf("写桥文件 %s: %v", bridge.Path, err)
		}
	}
}

// planAndGenerate 走「扫描 → 写桥 → 生成」的完整路径。
func planAndGenerate(t *testing.T, dir string) PackageResult {
	t.Helper()
	plan, err := Scan(dir)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	writeBridges(t, plan)
	result, err := Generate(plan)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return result
}

// TestGenerateRewritesBridgeCalls 锁住编排重写：三种语句形态都变成 ExecuteActivity+Get，
// 且 ActivityRef 的 Type/Queue/Version/Input/Output 来自注解。
func TestGenerateRewritesBridgeCalls(t *testing.T) {
	result := planAndGenerate(t, copyFixture(t))

	program := string(mustJSON(t, result.Workflow.Program))
	for _, want := range []string{
		`"symbol":"inventory.Reserve"`,
		`"symbol":"orders.ChargeV2"`,
		`"symbol":"payments.SettleRemoteActivity"`,
		`"symbol":"orders.Cash"`,
		`"symbol":"orders.Notify"`,
		`"ExecuteActivity"`,
		`"Get"`,
		// if 初始化里的 `_` 输出位：临时变量被提到 if 之前，程序里出现 bridgeOut。
		`"bridgeOut1"`,
		// 结构体字面量输入转成按 json tag 命名的 map 载荷。
		`"reservation_id"`,
	} {
		if !strings.Contains(program, want) {
			t.Fatalf("WLang 产物缺少 %s\n%s", want, program)
		}
	}
	for _, unwanted := range []string{`"CallCharge"`, `"CallReserve"`, `"CallSettle"`, `"CallNotify"`} {
		if strings.Contains(program, unwanted) {
			t.Fatalf("WLang 产物里仍有未重写的桥调用 %s\n%s", unwanted, program)
		}
	}

	byType := map[string]struct {
		queue   string
		version string
		input   string
		output  string
		symbol  string
	}{}
	for _, ref := range result.Activities {
		byType[ref.Type] = struct {
			queue   string
			version string
			input   string
			output  string
			symbol  string
		}{ref.TaskQueue, ref.Version, ref.Input, ref.Output, ref.Symbol}
	}
	cases := []struct {
		activityType string
		queue        string
		version      string
		symbol       string
	}{
		// 未显式写 queue 的 activity 继承 @Workflow(queue=orders)。
		{"inventory.reserve", "inventory", "v1", "inventory.Reserve"},
		{"orders.charge", "orders", "v1", "orders.Charge"},
		{"orders.charge-v2", "orders", "v2", "orders.ChargeV2"},
		{"orders.cash", "orders", "v1", "orders.Cash"},
		{"orders.notify", "orders", "v1", "orders.Notify"},
		{"payments.settle", "payments", "v1", "payments.SettleRemoteActivity"},
	}
	for _, expected := range cases {
		got, ok := byType[expected.activityType]
		if !ok {
			t.Fatalf("activities 清单缺少 %s: %+v", expected.activityType, result.Activities)
		}
		if got.queue != expected.queue || got.version != expected.version || got.symbol != expected.symbol {
			t.Fatalf("%s 的 queue/version/symbol = %s/%s/%s，期望 %s/%s/%s",
				expected.activityType, got.queue, got.version, got.symbol,
				expected.queue, expected.version, expected.symbol)
		}
		// Input/Output 是与 Go 包名/类型名无关的 wire 指纹（服务端与 worker 用同一算法）。
		for _, fingerprint := range []string{got.input, got.output} {
			if !strings.HasPrefix(fingerprint, "wire:") {
				t.Fatalf("%s 的 Input/Output 不是 wire 指纹: %q / %q", expected.activityType, got.input, got.output)
			}
		}
	}

	if result.Workflow.Policy != "auto_upgrade" || result.Workflow.Version != "v2" {
		t.Fatalf("WorkflowConfig policy/version = %q/%q，期望 auto_upgrade/v2", result.Workflow.Policy, result.Workflow.Version)
	}
	if result.Workflow.ID != "orders-demo" {
		t.Fatalf("workflow id = %q", result.Workflow.ID)
	}
}

// TestGenerateDoesNotDependOnGeneratedFiles 从零产物目录开始生成两次，要求字节稳定，
// 且生成的桥文件不再作为注解输入。
func TestGenerateDoesNotDependOnGeneratedFiles(t *testing.T) {
	dir := copyFixture(t)
	first := planAndGenerate(t, dir)
	firstBridge := readBridge(t, dir)
	second := planAndGenerate(t, dir)
	secondBridge := readBridge(t, dir)

	if workflowVersion(t, first) != workflowVersion(t, second) {
		t.Fatal("两次生成的配置内容版本不同")
	}
	if string(firstBridge) != string(secondBridge) {
		t.Fatal("两次生成的桥文件不一致")
	}
	// 删掉桥文件后重新生成：结果必须与第一次相同（桥文件不作为输入）。
	for _, bridge := range first.Bridges {
		if err := os.Remove(bridge.Path); err != nil {
			t.Fatal(err)
		}
	}
	third := planAndGenerate(t, dir)
	if workflowVersion(t, third) != workflowVersion(t, first) {
		t.Fatal("删除生成文件后配置内容版本漂移")
	}
	if string(readBridge(t, dir)) != string(firstBridge) {
		t.Fatal("删除生成文件后桥内容漂移")
	}
}

// readBridge 读取 fixture 目录里的桥文件。
func readBridge(t *testing.T, dir string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, "zz_orders_contract_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestBridgeFilesCoverLocalAndRemote 锁住桥/marker 形状。
func TestBridgeFilesCoverLocalAndRemote(t *testing.T) {
	result := planAndGenerate(t, copyFixture(t))
	content := string(readBridge(t, result.Dir))
	for _, want := range []string{
		"func CallCharge(ctx workflow.Context, in ChargeInput, opts ...workflow.ActivityOptions) (ChargeOutput, error)",
		"err := workflow.ExecuteActivity(ctx, Charge, in, opts...).Get(ctx, &out)",
		"var _ activity.Handler[ChargeInput, ChargeOutput] = Charge",
		"var _ activity.Caller[ChargeInput, ChargeOutput] = CallCharge",
	} {
		if !strings.Contains(content, want) {
			t.Fatalf("同包桥缺少 %q\n%s", want, content)
		}
	}
	var remoteBridge string
	for _, bridge := range result.Bridges {
		if strings.HasSuffix(bridge.Path, "zz_payments_contract_gen.go") {
			remoteBridge = string(bridge.Content)
		}
	}
	for _, want := range []string{
		"func SettleRemoteActivity(_ context.Context, _ SettleInput) (SettleOutput, error)",
		"func CallSettle(ctx workflow.Context, in SettleInput, opts ...workflow.ActivityOptions) (SettleOutput, error)",
		"workflow.ExecuteActivity(ctx, SettleRemoteActivity, in, opts...)",
	} {
		if !strings.Contains(remoteBridge, want) {
			t.Fatalf("remote 桥缺少 %q\n%s", want, remoteBridge)
		}
	}
	if strings.Contains(remoteBridge, "activity.Handler") {
		t.Fatal("remote 契约没有本地实现，不应生成 Handler 断言")
	}
}

// TestScanReportsAnnotationErrors 覆盖注解语法/缺字段/未知 key/未知 policy 的定位。
func TestScanReportsAnnotationErrors(t *testing.T) {
	cases := []struct {
		name    string
		source  string
		wantErr []string
	}{
		{
			name: "注解语法缺少右括号",
			source: `package bad

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

// @Workflow(id="bad",name="Bad"
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{"workflow.go:5", "缺少右括号"},
		},
		{
			name: "缺少 id",
			source: `package bad

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

// @Workflow(name="Bad")
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{"workflow.go:5", "缺少 id"},
		},
		{
			name: "未知 key",
			source: `package bad

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

// @Workflow(id="bad",retry=3)
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{"workflow.go:5", `未知 key "retry"`},
		},
		{
			name: "未知 policy",
			source: `package bad

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

// @Workflow(id="bad",policy=latest)
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{"workflow.go:5", `policy="latest" 未知`},
		},
		{
			name: "key 缺少等号",
			source: `package bad

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

// @Workflow(id "bad")
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{"workflow.go:5", "不是 key=value 形式"},
		},
		{
			name: "remote 标注在有函数体的实现上",
			source: `package bad

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// @Activity(type="bad.act",remote=true)
func A(ctx context.Context, in int64) (int64, error) { return in, nil }

// @Workflow(id="bad")
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{"workflow.go:9", "remote=true 只能标注命名函数类型"},
		},
		{
			name: "命名函数类型缺少 remote",
			source: `package bad

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// @Activity(type="bad.act")
type ARemote func(context.Context, int64) (int64, error)

// @Workflow(id="bad")
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{"workflow.go:9", "必须声明 remote=true"},
		},
		{
			name: "activity 签名不符合 ABI",
			source: `package bad

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// @Activity(type="bad.act")
func A(ctx context.Context, in int64) int64 { return in }

// @Workflow(id="bad")
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{"workflow.go:9", "必须返回 (out, error)"},
		},
		{
			name: "重复 activity type",
			source: `package bad

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// @Activity(type="bad.act")
func A(ctx context.Context, in int64) (int64, error) { return in, nil }

// @Activity(type="bad.act")
func B(ctx context.Context, in int64) (int64, error) { return in, nil }

// @Workflow(id="bad")
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{`@Activity type="bad.act" 重复声明`},
		},
		{
			// 同一个 registry key 只能有一个实现：同 type、不同 version 也必须拒绝，
			// 否则批量注册会静默覆盖前一个实现。
			name: "同 type 不同 version 重复声明",
			source: `package bad

import (
	"context"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// @Activity(type="bad.act",version=v1)
func A(ctx context.Context, in int64) (int64, error) { return in, nil }

// @Activity(type="bad.act",version=v2)
func B(ctx context.Context, in int64) (int64, error) { return in, nil }

// @Workflow(id="bad")
func W(ctx workflow.Context) error { return nil }`,
			wantErr: []string{`@Activity type="bad.act" 重复声明`},
		},
		{
			name: "重复 workflow id",
			source: `package bad

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

// @Workflow(id="dup")
func W1(ctx workflow.Context) error { return nil }

// @Workflow(id="dup")
func W2(ctx workflow.Context) error { return nil }`,
			wantErr: []string{"@Workflow 注解只能有一个"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			dir := writeBadPackage(t, testCase.source)
			_, err := Scan(dir)
			if err == nil {
				t.Fatal("期望报错，实际通过")
			}
			for _, want := range testCase.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("错误 %q 缺少 %q", err.Error(), want)
				}
			}
		})
	}
}

// writeBadPackage 在 testdata 下写一个只有单个源码文件的最小包。
func writeBadPackage(t *testing.T, source string) string {
	t.Helper()
	dir, err := os.MkdirTemp(filepath.Join("testdata"), "tmp-bad-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.WriteFile(filepath.Join(dir, "workflow.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

// mustJSON 把值序列化成便于断言的字符串。
func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func workflowVersion(t *testing.T, result PackageResult) string {
	t.Helper()
	version, err := programpkg.WorkflowConfigVersion(result.Workflow)
	if err != nil {
		t.Fatal(err)
	}
	return version
}
