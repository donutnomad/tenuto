// Package definitiontest 提供 definition.ConfigRepository 的契约测试，
// 各实现的测试用同一套断言运行。
package definitiontest

import (
	"errors"
	"slices"
	"testing"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
)

// RunConfigRepositoryContract 对 newRepo 返回的实现运行配置版本仓储契约；每个子测试拿到一个空仓储。
func RunConfigRepositoryContract(t *testing.T, newRepo func(t *testing.T) definition.ConfigRepository) {
	t.Run("SaveActivatesContentVersion", func(t *testing.T) { testSaveActivatesContentVersion(t, newRepo(t)) })
	t.Run("ArtifactKeepsActive", func(t *testing.T) { testArtifactKeepsActive(t, newRepo(t)) })
	t.Run("EnrollActivation", func(t *testing.T) { testEnrollActivation(t, newRepo(t)) })
	t.Run("EnrollState", func(t *testing.T) { testEnrollState(t, newRepo(t)) })
	t.Run("MissingConfig", func(t *testing.T) { testMissingConfig(t, newRepo(t)) })
	t.Run("NamespaceIsolation", func(t *testing.T) { testConfigNamespaceIsolation(t, newRepo(t)) })
	t.Run("ListRefs", func(t *testing.T) { testListRefs(t, newRepo(t)) })
	t.Run("SavedContentIsDetached", func(t *testing.T) { testSavedContentIsDetached(t, newRepo(t)) })
}

func mustVersion(t *testing.T, wf *programpkg.WorkflowConfig) string {
	t.Helper()
	version, err := programpkg.WorkflowConfigVersion(wf)
	if err != nil {
		t.Fatalf("content version: %v", err)
	}
	return version
}

func assertActive(t *testing.T, repo definition.ConfigRepository, namespace, workflowType, want string) {
	t.Helper()
	ref, err := repo.LatestWorkflowConfigRefInNamespace(namespace, workflowType)
	if err != nil {
		t.Fatalf("active ref of %s/%s: %v", namespace, workflowType, err)
	}
	if ref.Version != want {
		t.Fatalf("active version of %s/%s = %q, want %q", namespace, workflowType, ref.Version, want)
	}
}

func assertNoActive(t *testing.T, repo definition.ConfigRepository, namespace, workflowType string) {
	t.Helper()
	ref, err := repo.LatestWorkflowConfigRefInNamespace(namespace, workflowType)
	if !errors.Is(err, definition.ErrWorkflowConfigNotFound) {
		t.Fatalf("active ref of %s/%s = %+v err=%v, want ErrWorkflowConfigNotFound", namespace, workflowType, ref, err)
	}
}

func testSaveActivatesContentVersion(t *testing.T, repo definition.ConfigRepository) {
	// Happy Path: Version 是内容 hash；同内容重复保存得到同一引用；每次保存都把 active 指向本次内容。
	contentA := &programpkg.WorkflowConfig{Type: "flow", Name: "A", Version: "v1"}
	contentB := &programpkg.WorkflowConfig{Type: "flow", Name: "B", Version: "v1"}
	first, err := repo.SaveWorkflowConfigInNamespace("ns", contentA)
	if err != nil {
		t.Fatal(err)
	}
	want := definition.WorkflowConfigRef{Namespace: "ns", WorkflowType: "flow", Version: mustVersion(t, contentA)}
	if first != want {
		t.Fatalf("ref = %+v, want %+v", first, want)
	}
	again, err := repo.SaveWorkflowConfigInNamespace("ns", contentA)
	if err != nil {
		t.Fatal(err)
	}
	if again != first {
		t.Fatalf("same content produced refs %+v and %+v", first, again)
	}
	second, err := repo.SaveWorkflowConfigInNamespace("ns", contentB)
	if err != nil {
		t.Fatal(err)
	}
	if second.Version == first.Version {
		t.Fatal("different content must produce a different version")
	}
	assertActive(t, repo, "ns", "flow", second.Version)

	// 旧版本内容继续可读，读出的内容与保存时一致。
	loaded, err := repo.GetWorkflowConfig(first)
	if err != nil {
		t.Fatalf("pinned content is unreadable after publishing a new version: %v", err)
	}
	if loaded.Type != "flow" || loaded.Name != "A" || loaded.Version != "v1" {
		t.Fatalf("loaded content = %+v", loaded)
	}
	if got := mustVersion(t, loaded); got != first.Version {
		t.Fatalf("loaded content hashes to %q, want %q", got, first.Version)
	}

	// 显式保存总是激活：回滚到 A。
	rollback, err := repo.SaveWorkflowConfigInNamespace("ns", contentA)
	if err != nil {
		t.Fatal(err)
	}
	assertActive(t, repo, "ns", "flow", rollback.Version)
}

func testArtifactKeepsActive(t *testing.T, repo definition.ConfigRepository) {
	// Happy Path: artifact 登记只保存内容版本，active 保持原值。
	artifact := &programpkg.WorkflowConfig{Type: "flow", Name: "artifact"}
	ref, err := repo.SaveWorkflowConfigArtifactInNamespace("ns", artifact)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Version != mustVersion(t, artifact) {
		t.Fatalf("artifact version = %q, want content hash", ref.Version)
	}
	assertNoActive(t, repo, "ns", "flow")
	if _, err := repo.GetWorkflowConfig(ref); err != nil {
		t.Fatalf("artifact content: %v", err)
	}

	active, err := repo.SaveWorkflowConfigInNamespace("ns", &programpkg.WorkflowConfig{Type: "flow", Name: "active"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SaveWorkflowConfigArtifactInNamespace("ns", &programpkg.WorkflowConfig{Type: "flow", Name: "later artifact"}); err != nil {
		t.Fatal(err)
	}
	assertActive(t, repo, "ns", "flow", active.Version)
}

func testEnrollActivation(t *testing.T, repo definition.ConfigRepository) {
	// Happy Path: 新内容首次出现时激活；已存在内容重复登记时 active 保持原值；
	// active 为空时登记已存在内容会补激活。
	contentA := &programpkg.WorkflowConfig{Type: "enroll-flow", Name: "A", Version: "v1"}
	contentB := &programpkg.WorkflowConfig{Type: "enroll-flow", Name: "B", Version: "v1"}

	refA, activated, err := repo.EnrollWorkflowConfigInNamespace("ns", contentA)
	if err != nil {
		t.Fatalf("enroll A: %v", err)
	}
	if !activated {
		t.Fatal("first enroll of A must activate")
	}
	assertActive(t, repo, "ns", "enroll-flow", refA.Version)

	refB, activated, err := repo.EnrollWorkflowConfigInNamespace("ns", contentB)
	if err != nil {
		t.Fatalf("enroll B: %v", err)
	}
	if !activated || refB.Version == refA.Version {
		t.Fatalf("enroll B activated=%v version=%q, want a new active version", activated, refB.Version)
	}
	assertActive(t, repo, "ns", "enroll-flow", refB.Version)

	replayedA, activated, err := repo.EnrollWorkflowConfigInNamespace("ns", contentA)
	if err != nil {
		t.Fatalf("replay A: %v", err)
	}
	if activated || replayedA.Version != refA.Version {
		t.Fatalf("replay A activated=%v version=%q, want false/%q", activated, replayedA.Version, refA.Version)
	}
	assertActive(t, repo, "ns", "enroll-flow", refB.Version)

	artifactOnly := &programpkg.WorkflowConfig{Type: "artifact-flow", Name: "A", Version: "v1"}
	registered, err := repo.SaveWorkflowConfigArtifactInNamespace("ns", artifactOnly)
	if err != nil {
		t.Fatalf("register artifact: %v", err)
	}
	assertNoActive(t, repo, "ns", "artifact-flow")
	enrolled, activated, err := repo.EnrollWorkflowConfigInNamespace("ns", artifactOnly)
	if err != nil {
		t.Fatalf("enroll artifact-only content: %v", err)
	}
	if !activated || enrolled.Version != registered.Version {
		t.Fatalf("enroll with empty active activated=%v version=%q, want true/%q", activated, enrolled.Version, registered.Version)
	}
	assertActive(t, repo, "ns", "artifact-flow", registered.Version)
}

func testEnrollState(t *testing.T, repo definition.ConfigRepository) {
	// Happy Path: 登记状态在内容首次出现或 active 为空时为 true。
	state := func(workflow *programpkg.WorkflowConfig) bool {
		t.Helper()
		activating, err := repo.WorkflowConfigEnrollStateInNamespace("ns", definition.WorkflowConfigRef{
			Namespace: "ns", WorkflowType: workflow.Type, Version: mustVersion(t, workflow),
		})
		if err != nil {
			t.Fatalf("enroll state: %v", err)
		}
		return activating
	}
	active := &programpkg.WorkflowConfig{Type: "flow", Name: "A"}
	if !state(active) {
		t.Fatal("unknown content must report activating")
	}
	if _, err := repo.SaveWorkflowConfigInNamespace("ns", active); err != nil {
		t.Fatal(err)
	}
	if state(active) {
		t.Fatal("stored content with an active version must not report activating")
	}
	if !state(&programpkg.WorkflowConfig{Type: "flow", Name: "B"}) {
		t.Fatal("new content must report activating")
	}
	pending := &programpkg.WorkflowConfig{Type: "pending-flow", Name: "A"}
	if _, err := repo.SaveWorkflowConfigArtifactInNamespace("ns", pending); err != nil {
		t.Fatal(err)
	}
	if !state(pending) {
		t.Fatal("stored content without an active version must report activating")
	}
}

func testMissingConfig(t *testing.T, repo definition.ConfigRepository) {
	// Error Case: 不存在的版本与不存在的 active 都返回 ErrWorkflowConfigNotFound。
	_, err := repo.GetWorkflowConfig(definition.WorkflowConfigRef{Namespace: "ns", WorkflowType: "missing", Version: "v"})
	if !errors.Is(err, definition.ErrWorkflowConfigNotFound) {
		t.Fatalf("GetWorkflowConfig error = %v, want ErrWorkflowConfigNotFound", err)
	}
	assertNoActive(t, repo, "ns", "missing")
}

func testConfigNamespaceIsolation(t *testing.T, repo definition.ConfigRepository) {
	// Edge Case: 同一 Workflow Type 在两个 namespace 各自保存内容与 active；空 namespace 归入 default。
	refA, err := repo.SaveWorkflowConfigInNamespace("tenant-a", &programpkg.WorkflowConfig{Type: "payments", Name: "A"})
	if err != nil {
		t.Fatal(err)
	}
	refB, err := repo.SaveWorkflowConfigInNamespace("tenant-b", &programpkg.WorkflowConfig{Type: "payments", Name: "B"})
	if err != nil {
		t.Fatal(err)
	}
	assertActive(t, repo, "tenant-a", "payments", refA.Version)
	assertActive(t, repo, "tenant-b", "payments", refB.Version)
	assertNoActive(t, repo, "tenant-c", "payments")
	if _, err := repo.GetWorkflowConfig(definition.WorkflowConfigRef{Namespace: "tenant-b", WorkflowType: "payments", Version: refA.Version}); !errors.Is(err, definition.ErrWorkflowConfigNotFound) {
		t.Fatalf("tenant-a content read through tenant-b: err=%v", err)
	}

	refDefault, err := repo.SaveWorkflowConfigInNamespace("", &programpkg.WorkflowConfig{Type: "payments", Name: "D"})
	if err != nil {
		t.Fatal(err)
	}
	if refDefault.Namespace != protocol.DefaultNamespace {
		t.Fatalf("empty namespace stored as %q, want %q", refDefault.Namespace, protocol.DefaultNamespace)
	}
	assertActive(t, repo, protocol.DefaultNamespace, "payments", refDefault.Version)
	loaded, err := repo.GetWorkflowConfig(definition.WorkflowConfigRef{WorkflowType: "payments", Version: refDefault.Version})
	if err != nil || loaded.Name != "D" {
		t.Fatalf("read with empty namespace = %+v err=%v", loaded, err)
	}
}

func testListRefs(t *testing.T, repo definition.ConfigRepository) {
	// Happy Path: List 返回 namespace 内全部内容版本，Active 返回每个 workflow 的 active 引用。
	a1, err := repo.SaveWorkflowConfigInNamespace("ns", &programpkg.WorkflowConfig{Type: "a", Name: "1"})
	if err != nil {
		t.Fatal(err)
	}
	// 保存时间精确到毫秒；隔开两次保存，同一 workflow 内的先后由保存时间决定。
	time.Sleep(2 * time.Millisecond)
	a2, err := repo.SaveWorkflowConfigInNamespace("ns", &programpkg.WorkflowConfig{Type: "a", Name: "2"})
	if err != nil {
		t.Fatal(err)
	}
	b1, err := repo.SaveWorkflowConfigArtifactInNamespace("ns", &programpkg.WorkflowConfig{Type: "b", Name: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SaveWorkflowConfigInNamespace("other", &programpkg.WorkflowConfig{Type: "a", Name: "other"}); err != nil {
		t.Fatal(err)
	}

	c1, err := repo.SaveWorkflowConfigArtifactInNamespace("ns", &programpkg.WorkflowConfig{Type: "c", Name: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.SaveWorkflowConfigInNamespace("ns", &programpkg.WorkflowConfig{Type: "c", Name: "1"}); err != nil {
		t.Fatal(err)
	}

	// List 按 Workflow Type 升序，同一 workflow 内按首次保存的先后；Active 按 Workflow Type 升序。
	// 两个后端返回同一顺序；重复保存不改变已有版本的位置。
	listed, err := repo.ListWorkflowConfigRefsInNamespace("ns")
	assertRefs(t, "stored refs", listed, err, a1, a2, b1, c1)
	active, err := repo.ActiveWorkflowConfigRefsInNamespace("ns")
	assertRefs(t, "active refs", active, err, a2, c1)
	listed, err = repo.ListWorkflowConfigRefsInNamespace("empty")
	assertRefs(t, "stored refs of an empty namespace", listed, err)
	active, err = repo.ActiveWorkflowConfigRefsInNamespace("empty")
	assertRefs(t, "active refs of an empty namespace", active, err)
}

func assertRefs(t *testing.T, label string, got []definition.WorkflowConfigRef, err error, want ...definition.WorkflowConfigRef) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s = %+v, want %+v", label, got, want)
	}
}

// testSavedContentIsDetached：保存后调用方改动自己的对象，读到的仍是保存时的内容；重复读取得到同一份内容。
func testSavedContentIsDetached(t *testing.T, repo definition.ConfigRepository) {
	input := &programpkg.WorkflowConfig{
		Type: "flow", Name: "saved",
		Activities: map[string]programpkg.ActivityRef{"Charge": {Type: "charge", TaskQueue: "payments"}},
	}
	ref, err := repo.SaveWorkflowConfigInNamespace("ns", input)
	if err != nil {
		t.Fatal(err)
	}
	input.Name = "changed"
	input.Activities["Charge"] = programpkg.ActivityRef{Type: "refund"}
	for range 2 {
		loaded, err := repo.GetWorkflowConfig(ref)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.Name != "saved" || loaded.Activities["Charge"].Type != "charge" {
			t.Fatalf("loaded content = %+v, want the content at save time", loaded)
		}
		if got := mustVersion(t, loaded); got != ref.Version {
			t.Fatalf("loaded content hashes to %q, want %q", got, ref.Version)
		}
	}
}
