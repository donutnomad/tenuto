package memory_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/definition/definitiontest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestDefinitionConfigRepositoryContract(t *testing.T) {
	definitiontest.RunConfigRepositoryContract(t, func(*testing.T) definition.ConfigRepository { return memory.NewDefinitionRepository() })
}

// BenchmarkGetWorkflowConfig 读取 cluster-order 的内容版本：每个 Workflow Task 与 Activity 开始都会读一次。
func BenchmarkGetWorkflowConfig(b *testing.B) {
	data, err := os.ReadFile("../../../cmd/tenuto-cluster/workload/order/workflow.json")
	if err != nil {
		b.Fatal(err)
	}
	var wf program.WorkflowConfig
	if err := json.Unmarshal(data, &wf); err != nil {
		b.Fatal(err)
	}
	repo := memory.NewDefinitionRepository()
	ref, err := repo.SaveWorkflowConfigInNamespace("ns", &wf)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := repo.GetWorkflowConfig(ref); err != nil {
			b.Fatal(err)
		}
	}
}
