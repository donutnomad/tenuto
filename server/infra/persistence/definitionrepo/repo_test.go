package definitionrepo

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

func TestMain(m *testing.M) {
	code := m.Run()
	mysqltest.Terminate()
	os.Exit(code)
}

// TestContentVersionSharedAcrossRuns 固定 Pinned 的存储事实：
// version 是 config JSON 的内容 hash；同一 (namespace, workflow_id, version) 只有一行 JSON，
// 重复登记幂等；多个 Run 只保存版本字符串引用，不复制内容。
func TestContentVersionSharedAcrossRuns(t *testing.T) {
	database := mysqltest.NewDB(t)
	configs := New(database)
	runs, err := executionrepo.New(database, executionrepo.Config{})
	if err != nil {
		t.Fatal(err)
	}

	workflow := &program.WorkflowConfig{Type: "shared-flow", Name: "A", Version: "v1", Program: programtest.Noop()}
	first, err := configs.SaveWorkflowConfigInNamespace("ns", workflow)
	if err != nil {
		t.Fatal(err)
	}
	// 重复登记同一内容：版本不变，JSON 不新增副本。
	second, err := configs.SaveWorkflowConfigInNamespace("ns", workflow)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("same content produced refs %+v and %+v", first, second)
	}
	var contentRows int64
	if err := database.Model(&WorkflowConfigPO{}).
		Where("namespace = ? AND workflow_id = ? AND version = ?", "ns", "shared-flow", first.Version).
		Count(&contentRows).Error; err != nil {
		t.Fatal(err)
	}
	if contentRows != 1 {
		t.Fatalf("content rows for one version = %d, want 1", contentRows)
	}

	// 两个 Run 引用同一个内容版本；Run 行只存版本字符串。
	for _, runID := range []string{"run-shared-a", "run-shared-b"} {
		if err := runs.RunInTx(func(tx execution.ExecutionTx) error {
			return tx.CreateRun(rootBranch(t, &execution.Run{
				ID: runID, Namespace: "ns", WorkflowID: "shared-flow", WorkflowType: "shared-flow",
				WorkflowConfigVersion: first.Version,
				Status:                history.StatusRunning, CreatedAt: time.Now(),
			}))
		}); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := runs.Get("run-shared-b")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.WorkflowConfigVersion != first.Version {
		t.Fatalf("run stores version %q, want shared %q", loaded.WorkflowConfigVersion, first.Version)
	}
	content, err := configs.GetWorkflowConfig(first)
	if err != nil {
		t.Fatal(err)
	}
	if content.Name != "A" {
		t.Fatalf("shared content name = %q, want A", content.Name)
	}
	versionKey, err := program.WorkflowConfigVersion(workflow)
	if err != nil {
		t.Fatal(err)
	}
	if versionKey != first.Version {
		t.Fatalf("version key = %q, want content hash %q", versionKey, first.Version)
	}

	// 版本标签不同即内容不同，产生新行；旧的 A 行继续供已有 Run 读取。
	renamed := *workflow
	renamed.Name = "B"
	third, err := configs.SaveWorkflowConfigInNamespace("ns", &renamed)
	if err != nil {
		t.Fatal(err)
	}
	if third.Version == first.Version {
		t.Fatal("different content must produce a different version")
	}
	var allRows int64
	if err := database.Model(&WorkflowConfigPO{}).Count(&allRows).Error; err != nil {
		t.Fatal(err)
	}
	if allRows != 2 {
		t.Fatalf("total content rows = %d, want 2 (A + B)", allRows)
	}
	if _, err := configs.GetWorkflowConfig(first); err != nil {
		t.Fatalf("existing pinned content is no longer readable after publishing B: %v", err)
	}
}

// TestEnrollGormRules 在 MySQL 上的生产仓库锁住
// 启动期自动登记的激活语义：新内容首次出现激活；同 hash 重复/旧内容重启保持 latest；
// 重复内容只存一份；首次登记时 latest 为空也补激活；显式保存仍总是激活（回滚）。
func TestEnrollGormRules(t *testing.T) {
	database := mysqltest.NewDB(t)
	configs := New(database)
	const namespace = "ns"

	assertActive := func(t *testing.T, workflowID, want string) {
		t.Helper()
		ref, err := configs.LatestWorkflowConfigRefInNamespace(namespace, workflowID)
		if err != nil {
			t.Fatalf("读取 %s latest: %v", workflowID, err)
		}
		if ref.Version != want {
			t.Fatalf("%s latest = %q, want %q", workflowID, ref.Version, want)
		}
	}
	assertNoActive := func(t *testing.T, workflowID string) {
		t.Helper()
		if ref, err := configs.LatestWorkflowConfigRefInNamespace(namespace, workflowID); err == nil {
			t.Fatalf("%s 不应有 latest，实际 %q", workflowID, ref.Version)
		}
	}

	contentA := &program.WorkflowConfig{Type: "enroll-flow", Name: "A", Version: "v1", Program: programtest.Noop()}
	contentB := &program.WorkflowConfig{Type: "enroll-flow", Name: "B", Version: "v1", Program: programtest.Noop()}

	// 空服务首次登记 A：内容新建，latest 指向 A。
	refA, activated, err := configs.EnrollWorkflowConfigInNamespace(namespace, contentA)
	if err != nil {
		t.Fatalf("enroll A: %v", err)
	}
	if !activated {
		t.Fatal("首次登记 A 应激活")
	}
	assertActive(t, "enroll-flow", refA.Version)

	// 新 hash 的 B：内容新建，latest 指向 B。
	refB, activated, err := configs.EnrollWorkflowConfigInNamespace(namespace, contentB)
	if err != nil {
		t.Fatalf("enroll B: %v", err)
	}
	if !activated || refB.Version == refA.Version {
		t.Fatalf("B 应新建并激活，activated=%v version=%q", activated, refB.Version)
	}
	assertActive(t, "enroll-flow", refB.Version)

	// 旧 binary 重启：A 的内容重复出现，不新建也不改 latest。
	replayedA, activated, err := configs.EnrollWorkflowConfigInNamespace(namespace, contentA)
	if err != nil {
		t.Fatalf("replay A: %v", err)
	}
	if activated || replayedA.Version != refA.Version {
		t.Fatalf("旧内容重复登记 activated=%v version=%q, want false/%q", activated, replayedA.Version, refA.Version)
	}
	assertActive(t, "enroll-flow", refB.Version)

	// 重复 B 的内容不产生第二行。
	replayedB, activated, err := configs.EnrollWorkflowConfigInNamespace(namespace, contentB)
	if err != nil {
		t.Fatalf("replay B: %v", err)
	}
	if activated || replayedB.Version != refB.Version {
		t.Fatalf("重复内容 activated=%v version=%q, want false/%q", activated, replayedB.Version, refB.Version)
	}
	var contentRows int64
	if err := database.Model(&WorkflowConfigPO{}).
		Where("namespace = ? AND workflow_id = ?", namespace, "enroll-flow").
		Count(&contentRows).Error; err != nil {
		t.Fatal(err)
	}
	if contentRows != 2 {
		t.Fatalf("enroll-flow 内容行数 = %d, want 2 (A + B)", contentRows)
	}

	// 显式保存保持「总是激活」：回滚到 A 仍然可用。
	rollback, err := configs.SaveWorkflowConfigInNamespace(namespace, contentA)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	assertActive(t, "enroll-flow", rollback.Version)

	// 先 artifact-only（latest 为空），随后首次 startup 的 enroll 补激活同一内容。
	artifactOnly := &program.WorkflowConfig{Type: "artifact-flow", Name: "A", Version: "v1", Program: programtest.Noop()}
	registered, err := configs.SaveWorkflowConfigArtifactInNamespace(namespace, artifactOnly)
	if err != nil {
		t.Fatalf("register artifact: %v", err)
	}
	assertNoActive(t, "artifact-flow")
	enrolled, activated, err := configs.EnrollWorkflowConfigInNamespace(namespace, artifactOnly)
	if err != nil {
		t.Fatalf("enroll artifact-only content: %v", err)
	}
	if !activated || enrolled.Version != registered.Version {
		t.Fatalf("空 latest 补激活 activated=%v version=%q, want true/%q", activated, enrolled.Version, registered.Version)
	}
	assertActive(t, "artifact-flow", registered.Version)

	// EnrollState 复用版本 hash + 精确版本读取 + active 读取：启动期据此决定
	// 是否做路由校验，避免旧内容重启依赖 Activity 在线。
	state := func(workflow *program.WorkflowConfig) bool {
		t.Helper()
		version, err := program.WorkflowConfigVersion(workflow)
		if err != nil {
			t.Fatalf("计算内容版本: %v", err)
		}
		activating, err := configs.WorkflowConfigEnrollStateInNamespace(namespace, definition.WorkflowConfigRef{
			Namespace: namespace, WorkflowID: workflow.Type, Version: version,
		})
		if err != nil {
			t.Fatalf("读取登记状态: %v", err)
		}
		return activating
	}
	if state(contentA) {
		t.Fatal("已存在内容 + 已有 latest 不应需要激活")
	}
	if !state(&program.WorkflowConfig{Type: "enroll-flow", Name: "C", Version: "v1", Program: programtest.Noop()}) {
		t.Fatal("新内容应需要激活")
	}
	pending := &program.WorkflowConfig{Type: "pending-flow", Name: "A", Version: "v1", Program: programtest.Noop()}
	if _, err := configs.SaveWorkflowConfigArtifactInNamespace(namespace, pending); err != nil {
		t.Fatalf("register pending artifact: %v", err)
	}
	if !state(pending) {
		t.Fatal("已登记但无 latest 应需要补激活")
	}
}

// rootBranch 给测试 Run 补上 root branch token：tree 与 branch 都取 Run ID，
// 与 execution.PrepareStartedRun 为新 Run 生成的 token 一致。
func rootBranch(t testing.TB, run *execution.Run) *execution.Run {
	t.Helper()
	token, err := execution.SerializeBranchToken(&execution.HistoryBranch{TreeID: run.ID, BranchID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	run.BranchToken = token
	return run
}

// TestGetWorkflowConfigReadsEachContentVersionOnce 固定内容版本缓存的行为：同一个内容版本第二次读取不发 SELECT，
// 返回同一个只读对象；latest 指针每次照常读库。
func TestGetWorkflowConfigReadsEachContentVersionOnce(t *testing.T) {
	database := mysqltest.NewDB(t)
	configs := New(database)
	ref, err := configs.SaveWorkflowConfigInNamespace("ns", &program.WorkflowConfig{Type: "cached-flow", Name: "A", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	// gsql 的查询经 GORM 的 Raw/Row 回调执行，Query 回调覆盖 gorm 链式查询：三者都计数。
	var selects int
	count := func(*gorm.DB) { selects++ }
	callbacks := database.Callback()
	for _, err := range []error{
		callbacks.Query().Before("gorm:query").Register("test:count_config_query", count),
		callbacks.Raw().Before("gorm:raw").Register("test:count_config_raw", count),
		callbacks.Row().Before("gorm:row").Register("test:count_config_row", count),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}

	first, err := configs.GetWorkflowConfig(ref)
	if err != nil {
		t.Fatal(err)
	}
	second, err := configs.GetWorkflowConfig(ref)
	if err != nil {
		t.Fatal(err)
	}
	if selects != 1 {
		t.Fatalf("SELECTs for two reads of one content version = %d, want 1", selects)
	}
	// 内容版本不可变：两次读取共享同一个只读对象，调用方不得修改它。
	if second != first || second.Name != "A" {
		t.Fatalf("second read = %p (name %q), want the shared object %p", second, second.Name, first)
	}
	if _, err := configs.LatestWorkflowConfigRefInNamespace("ns", "cached-flow"); err != nil {
		t.Fatal(err)
	}
	if selects != 2 {
		t.Fatalf("latest ref was served without reading the store: SELECTs = %d, want 2", selects)
	}
	if _, err := configs.GetWorkflowConfig(definition.WorkflowConfigRef{Namespace: "ns", WorkflowID: "cached-flow", Version: "missing"}); err == nil {
		t.Fatal("missing content version was found")
	}
}

func TestConfigContentsEvictsOldestBeyondLimit(t *testing.T) {
	var contents configContents
	for i := range configContentsLimit + 1 {
		contents.put(definition.WorkflowConfigRef{Namespace: "ns", WorkflowID: "wf", Version: fmt.Sprint(i)}, &program.WorkflowConfig{})
	}
	if _, ok := contents.get(definition.WorkflowConfigRef{Namespace: "ns", WorkflowID: "wf", Version: "0"}); ok {
		t.Fatal("oldest content version survived past the limit")
	}
	if _, ok := contents.get(definition.WorkflowConfigRef{Namespace: "ns", WorkflowID: "wf", Version: fmt.Sprint(configContentsLimit)}); !ok {
		t.Fatal("newest content version was evicted")
	}
	if len(contents.entries) != configContentsLimit || len(contents.order) != configContentsLimit {
		t.Fatalf("cache size = %d/%d, want %d", len(contents.entries), len(contents.order), configContentsLimit)
	}
}
