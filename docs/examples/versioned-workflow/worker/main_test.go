package main

import (
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/compiler/artifact"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
)

// TestWorkerReportsActivityContracts 锁住 worker 侧的契约元数据上报：
// 实现按 workflow.json 里 activities 的符号注册，registry 里每个 activity 都带 Type/Version/Input/Output。
func TestWorkerReportsActivityContracts(t *testing.T) {
	artifacts, err := artifact.LoadDir("../v2")
	if err != nil {
		t.Fatalf("加载 workflow.json 失败: %v", err)
	}
	if len(artifacts.Workflow.Activities) == 0 {
		t.Fatal("workflow.json 的 activities 为空，先生成产物")
	}
	registry := wfsdk.NewRegistry()
	if err := registerActivities(registry, artifacts.Workflow.Activities); err != nil {
		t.Fatalf("注册 activity 失败: %v", err)
	}

	reported := registry.Contracts()
	if len(reported) != 5 {
		t.Fatalf("应上报 5 个契约（4 个本地 + 1 个 remote），实际 %d: %+v", len(reported), reported)
	}
	registered := map[string]bool{}
	for _, activityType := range registry.Types() {
		registered[activityType] = true
	}
	for _, contract := range reported {
		if contract.Type == "" || contract.Version == "" || contract.Input == "" || contract.Output == "" {
			t.Fatalf("契约字段不完整: %+v", contract)
		}
		// worker 上报的是与 Go 类型身份无关的 wire 指纹，不是 descriptor ID。
		if !strings.HasPrefix(contract.Input, "wire:") || !strings.HasPrefix(contract.Output, "wire:") {
			t.Fatalf("%s 上报的 input/output 不是 wire 指纹: %+v", contract.Type, contract)
		}
		if !registered[contract.Type] {
			t.Fatalf("契约 %s 没有对应的实现注册（registry: %v）", contract.Type, registered)
		}
	}
	// payments.settle 是 remote 契约，实现由 worker 提供。
	var settle *wfsdk.Contract
	for index := range reported {
		if reported[index].Type == "payments.settle" {
			settle = &reported[index]
		}
	}
	if settle == nil {
		t.Fatalf("remote 契约 payments.settle 没有上报: %+v", reported)
	}
	if settle.Version != "v1" || !strings.HasPrefix(settle.Input, "wire:") || !strings.HasPrefix(settle.Output, "wire:") {
		t.Fatalf("remote 契约元数据异常: %+v", *settle)
	}
}

// TestWorkerRejectsMissingManifestSymbol 锁住「清单缺符号时快速失败」。
func TestWorkerRejectsMissingManifestSymbol(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("缺少清单符号应 panic（fail fast）")
		}
	}()
	_ = registerActivities(wfsdk.NewRegistry(), nil)
}
