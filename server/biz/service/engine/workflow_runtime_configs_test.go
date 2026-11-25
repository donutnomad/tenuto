package engine

import (
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

func TestListWorkflowConfigs_Empty(t *testing.T) {
	// Happy Path: namespace 下没有注册任何 workflow 时返回空列表
	server := newTestServer()

	result, err := server.View().ListWorkflowConfigs("default")
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 0 {
		t.Fatalf("expected 0 items, got %d", result.Total)
	}
	if len(result.Items) != 0 {
		t.Fatalf("expected empty items, got %v", result.Items)
	}
}

func TestListWorkflowConfigs_SingleWorkflow(t *testing.T) {
	// Happy Path: 注册一个 workflow 后可正确列出
	server := newTestServer()

	_, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "order-flow",
		Name:    "Order Flow",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"step-1": {Symbol: "step-1", Type: "validate"},
			"step-2": {Symbol: "step-2", Type: "process"},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	result, err := server.View().ListWorkflowConfigs("default")
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 {
		t.Fatalf("expected 1 item, got %d", result.Total)
	}
	item := result.Items[0]
	if item.WorkflowID != "order-flow" {
		t.Fatalf("expected workflow_id=order-flow, got %s", item.WorkflowID)
	}
	if item.Name != "Order Flow" {
		t.Fatalf("expected name=Order Flow, got %s", item.Name)
	}
	if item.ActivityCount != 2 {
		t.Fatalf("expected activity_count=2, got %d", item.ActivityCount)
	}
	if !item.Active {
		t.Fatal("expected workflow to be active")
	}
	if item.VersionCount < 1 {
		t.Fatalf("expected version_count >= 1, got %d", item.VersionCount)
	}
	if item.LatestVersion == "" {
		t.Fatal("expected non-empty latest_version")
	}
}

func TestListWorkflowConfigs_MultipleWorkflows(t *testing.T) {
	// Happy Path: 多个 workflow 按 workflow_id 排序返回
	server := newTestServer()

	workflows := []program.WorkflowConfig{
		{Type: "beta-flow", Name: "Beta", Program: programtest.Noop()},
		{Type: "alpha-flow", Name: "Alpha", Program: programtest.Noop()},
		{Type: "gamma-flow", Name: "Gamma", Program: programtest.Noop()},
	}
	for i := range workflows {
		if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &workflows[i]); err != nil {
			t.Fatal(err)
		}
	}

	result, err := server.View().ListWorkflowConfigs("default")
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 3 {
		t.Fatalf("expected 3 items, got %d", result.Total)
	}
	if result.Items[0].WorkflowID != "alpha-flow" {
		t.Fatalf("expected first item alpha-flow, got %s", result.Items[0].WorkflowID)
	}
	if result.Items[1].WorkflowID != "beta-flow" {
		t.Fatalf("expected second item beta-flow, got %s", result.Items[1].WorkflowID)
	}
	if result.Items[2].WorkflowID != "gamma-flow" {
		t.Fatalf("expected third item gamma-flow, got %s", result.Items[2].WorkflowID)
	}
}

func TestListWorkflowConfigs_NamespaceIsolation(t *testing.T) {
	// Edge Case: 不同 namespace 的 workflow 互相隔离
	server := newTestServer()

	if _, err := server.Definitions().RegisterWorkflowInNamespace("ns-a", &program.WorkflowConfig{Type: "wf-a", Name: "A", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace("ns-b", &program.WorkflowConfig{Type: "wf-b", Name: "B", Program: programtest.Noop()}); err != nil {
		t.Fatal(err)
	}

	resultA, err := server.View().ListWorkflowConfigs("ns-a")
	if err != nil {
		t.Fatal(err)
	}
	if resultA.Total != 1 {
		t.Fatalf("ns-a: expected 1 item, got %d", resultA.Total)
	}
	if resultA.Items[0].WorkflowID != "wf-a" {
		t.Fatalf("ns-a: expected wf-a, got %s", resultA.Items[0].WorkflowID)
	}

	resultB, err := server.View().ListWorkflowConfigs("ns-b")
	if err != nil {
		t.Fatal(err)
	}
	if resultB.Total != 1 {
		t.Fatalf("ns-b: expected 1 item, got %d", resultB.Total)
	}
	if resultB.Items[0].WorkflowID != "wf-b" {
		t.Fatalf("ns-b: expected wf-b, got %s", resultB.Items[0].WorkflowID)
	}
}

func TestListWorkflowConfigs_MultipleVersions(t *testing.T) {
	// Happy Path: 同一 workflow 注册多个版本时 version_count 正确
	server := newTestServer()

	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "versioned-flow",
		Name:    "V1",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"s1": {Symbol: "s1", Type: "a"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, &program.WorkflowConfig{
		Type:    "versioned-flow",
		Name:    "V2",
		Program: programtest.Noop(),
		Activities: map[string]program.ActivityRef{
			"s1": {Symbol: "s1", Type: "a"},
			"s2": {Symbol: "s2", Type: "b"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := server.View().ListWorkflowConfigs("default")
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 {
		t.Fatalf("expected 1 workflow, got %d", result.Total)
	}
	if result.Items[0].VersionCount < 2 {
		t.Fatalf("expected version_count >= 2, got %d", result.Items[0].VersionCount)
	}
}

func newTestServer() *Server {
	mgr := memory.NewExecutionManager()
	wfQueue := newTestTaskQueue()
	actQueue := newTestTaskQueue()
	return assembleServer(mgr, wfQueue, actQueue)
}
