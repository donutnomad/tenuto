package integration

import (
	"context"
	"testing"

	"gorm.io/gorm"

	"github.com/donutnomad/tenuto/compiler/sdkgen/sdkgentest"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// 这两个用例用真实 Server 驱动 GORM Store，验证 config 版本在持久化后被新 run 与重启后的 Server 采用。

func TestServerRunsRegisteredConfigVersions(t *testing.T) {
	store, db := newGCStore(t, 0)
	workflowTasks := memory.NewTaskQueue(16)
	activityTasks := memory.NewActivityQueueRouter(16)
	server := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(store), WorkflowQueue: workflowTasks, ActivityRouter: activityTasks})
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "wf-versioned"},
		sdkgentest.Step{Type: "complete", Input: `map[string]any{"value": "v1"}`, Options: `StartToCloseTimeout: time.Minute`},
	)

	ctx := context.Background()

	firstRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}
	// 第一个 run pin 到首次注册版本。
	firstRunID, err := startedRunID(server.StartWorkflow(ctx, "", wf.Type, wf.Type, "", map[string]any{"request": "first"}))
	if err != nil {
		t.Fatal(err)
	}
	firstRun, err := store.Executions.Get(firstRunID)
	if err != nil {
		t.Fatal(err)
	}
	if firstRun.WorkflowConfigVersion != firstRef.Version {
		t.Fatalf("first run should reference registered version %q, got %q", firstRef.Version, firstRun.WorkflowConfigVersion)
	}
	assertWorkflowConfigRows(t, db, 1)

	// 重新注册同 workflowID（内容变化）产生新版本：config 行数增长，且新版本 ref 与旧不同。
	wf.Name = "v2"
	secondRef, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}
	if secondRef.Version == firstRef.Version {
		t.Fatalf("re-register with changed content should produce a new version, both %q", secondRef.Version)
	}
	assertWorkflowConfigRows(t, db, 2)

	// 同一 workflowID 至多一个 current/running run（对标 Temporal）：要起第二个 run 验证它 pin 到
	// 新版本，须先让上一个 run 终态、让出 current。终止第一个 run（让位），再以切代语义起第二个 run
	// （StartWorkflowForCron：从上一代 current 切代，并解析到 active 注册版本），断言它实际 pin 到
	// 新版本 secondRef —— 这是「新 run 落在新 config 版本」的核心断言。
	if err := server.TerminateWorkflow(context.Background(), "", "", firstRunID, "make-way-for-new-run"); err != nil {
		t.Fatal(err)
	}
	secondRunID, err := server.StartWorkflowForCron(ctx, "", wf.Type, wf.Type, map[string]any{"request": "second"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if secondRunID == firstRunID {
		t.Fatalf("expected a distinct second run, got same id %q", secondRunID)
	}
	secondRun, err := store.Executions.Get(secondRunID)
	if err != nil {
		t.Fatal(err)
	}
	if secondRun.WorkflowConfigVersion != secondRef.Version {
		t.Fatalf("second run should pin to the newly registered version %q, got %q", secondRef.Version, secondRun.WorkflowConfigVersion)
	}
	// 第二个 run 成为 current，第一个 run 已让出 current。
	cur, err := store.Executions.GetCurrentRunID("", wf.Type)
	if err != nil {
		t.Fatal(err)
	}
	if cur != secondRunID {
		t.Fatalf("current should switch to second run %q, got %q", secondRunID, cur)
	}
}

func TestServerStartsLatestPersistedWorkflowConfigAfterRestart(t *testing.T) {
	store, _ := newGCStore(t, 0)
	firstServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(store), WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	wf := sdkgentest.Sequential(t, &program.WorkflowConfig{Type: "wf-persisted-latest"},
		sdkgentest.Step{Type: "complete", Input: `map[string]any{"value": "v1"}`, Options: `StartToCloseTimeout: time.Minute`},
	)
	if _, err := firstServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf); err != nil {
		t.Fatal(err)
	}
	wf.Name = "v2"
	latestRef, err := firstServer.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wf)
	if err != nil {
		t.Fatal(err)
	}

	restartedServer := assembly.NewServer(assembly.Options{Stores: assembly.Persistent(store), WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	runID, err := startedRunID(restartedServer.StartWorkflow(context.Background(), "", wf.Type, wf.Type, "", map[string]any{"request": "after-restart"}))
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.Executions.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.WorkflowConfigVersion != latestRef.Version {
		t.Fatalf("run should reference latest persisted version %q, got %q", latestRef.Version, run.WorkflowConfigVersion)
	}
	loaded, err := restartedServer.GetWorkflowConfig(context.Background(), runID, "")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Name != "v2" {
		t.Fatalf("expected latest persisted workflow config, got %+v", loaded)
	}
}

func assertWorkflowConfigRows(t *testing.T, db *gorm.DB, want int64) {
	t.Helper()
	var count int64
	if err := db.Table("tenuto_configs").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("expected %d persisted workflow configs, got %d", want, count)
	}
}
