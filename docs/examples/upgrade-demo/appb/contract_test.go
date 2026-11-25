package appb_test

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	appav1 "github.com/donutnomad/tenuto/docs/examples/upgrade-demo/appa/v1"
	appav2 "github.com/donutnomad/tenuto/docs/examples/upgrade-demo/appa/v2"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/activity"
)

// app-b 声明的远程契约（生成产物 workflow.json 的 activities 里 fulfillment.* 的条目）与 app-a 同一版本
// Worker 上报的契约（registry.Contracts()）逐项相等。两个应用各自定义 DTO，
// 这个测试是它们在发布之前保持一致的手段；Server 在调度时做同样的对照。

func TestRemoteContractsMatchAppA(t *testing.T) {
	for _, tt := range []struct {
		name     string
		declared string
		register func(*activity.Registry) error
	}{
		{name: "app-b v1 against app-a v1", declared: "v1/workflow.json", register: appav1.RegisterActivities},
		{name: "app-b v2 against app-a v2", declared: "v2/workflow.json", register: appav2.RegisterActivities},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if diff := mismatches(t, tt.declared, tt.register); len(diff) > 0 {
				t.Fatalf("remote contracts differ from app-a: %v", diff)
			}
		})
	}
}

// app-b v1 的声明对上 app-a v2 的实现，正好差在这次发布改动的两个 Activity 上：
// reserve 的入参指纹、quote 的 version。fulfillment.ship 没有变化。
func TestUpgradeChangesExactlyTwoContracts(t *testing.T) {
	diff := mismatches(t, "v1/workflow.json", appav2.RegisterActivities)
	want := []string{"fulfillment.quote: version", "fulfillment.reserve: input"}
	if strings.Join(diff, "; ") != strings.Join(want, "; ") {
		t.Fatalf("mismatches = %v, want %v", diff, want)
	}
}

// mismatches 返回 declared 里每个 fulfillment.* 契约与 app-a 上报值不相等的维度，按 type 排序。
func mismatches(t *testing.T, declaredPath string, register func(*activity.Registry) error) []string {
	t.Helper()
	data, err := os.ReadFile(declaredPath)
	if err != nil {
		t.Fatal(err)
	}
	var workflow program.WorkflowConfig
	if err := json.Unmarshal(data, &workflow); err != nil {
		t.Fatal(err)
	}
	declared := make([]protocol.ActivityContract, 0, len(workflow.Activities))
	for _, ref := range workflow.Activities {
		declared = append(declared, protocol.ActivityContract{Type: ref.Type, Version: ref.Version, Input: ref.Input, Output: ref.Output})
	}
	sort.Slice(declared, func(i, j int) bool { return declared[i].Type < declared[j].Type })
	registry := activity.NewRegistry()
	if err := register(registry); err != nil {
		t.Fatal(err)
	}
	reported := map[string]protocol.ActivityContract{}
	for _, contract := range registry.Contracts() {
		reported[contract.Type] = contract
	}
	var diff []string
	for _, contract := range declared {
		if !strings.HasPrefix(contract.Type, "fulfillment.") {
			continue
		}
		worker, ok := reported[contract.Type]
		if !ok {
			diff = append(diff, contract.Type+": not implemented")
			continue
		}
		for _, field := range protocol.CompareActivityContracts(contract, []protocol.ActivityContract{worker}) {
			diff = append(diff, contract.Type+": "+field.Field)
		}
	}
	return diff
}
