package engine

import (
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// TestEnrollWorkflowRuleA 锁住启动期自动登记的激活规则：
// 新内容首次出现 → latest 跟随；同 hash 重复出现（旧 binary 重启）→ 保持 latest；
// 重复内容只存一份；显式 register（回滚）仍然总是激活；坏配置不改 latest。
func TestEnrollWorkflowRuleA(t *testing.T) {
	servers := map[string]*Server{
		"local store":  newTestServer(),
		"memory store": assembleServer(memory.NewExecutionManager(), memory.NewTaskQueue(32), memory.NewTaskQueue(32)),
	}
	for name, server := range servers {
		t.Run(name, func(t *testing.T) {
			const namespace = "enroll"
			contentA := &program.WorkflowConfig{ID: "enroll-flow", Name: "A", Program: programtest.Noop()}
			contentB := &program.WorkflowConfig{ID: "enroll-flow", Name: "B", Program: programtest.Noop()}

			refA, err := server.Definitions().EnrollWorkflowInNamespace(namespace, contentA)
			if err != nil {
				t.Fatalf("enroll A: %v", err)
			}
			assertActiveVersion(t, server, namespace, "enroll-flow", refA.Version)

			refB, err := server.Definitions().EnrollWorkflowInNamespace(namespace, contentB)
			if err != nil {
				t.Fatalf("enroll B: %v", err)
			}
			if refB.Version == refA.Version {
				t.Fatal("A 与 B 的内容版本相同")
			}
			assertActiveVersion(t, server, namespace, "enroll-flow", refB.Version)

			// 旧 binary 重启：A 的内容重复出现，latest 必须保持 B。
			replayedA, err := server.Definitions().EnrollWorkflowInNamespace(namespace, contentA)
			if err != nil {
				t.Fatalf("replay A: %v", err)
			}
			if replayedA.Version != refA.Version {
				t.Fatalf("重复内容的版本 = %q, want %q", replayedA.Version, refA.Version)
			}
			assertActiveVersion(t, server, namespace, "enroll-flow", refB.Version)

			// 重复登记 B 的内容不产生第二个版本。
			if _, err := server.Definitions().EnrollWorkflowInNamespace(namespace, contentB); err != nil {
				t.Fatalf("replay B: %v", err)
			}
			if refs, err := server.workflowConfigs.ListWorkflowConfigRefsInNamespace(namespace); err != nil || len(refs) != 2 {
				t.Fatalf("内容版本数 = %d, want 2: %+v", len(refs), refs)
			}
			if _, err := server.workflowConfigs.GetWorkflowConfig(refA); err != nil {
				t.Fatalf("A 的内容版本不可读: %v", err)
			}
			if _, err := server.workflowConfigs.GetWorkflowConfig(refB); err != nil {
				t.Fatalf("B 的内容版本不可读: %v", err)
			}

			// 显式注册保持「总是激活」：回滚到 A 仍然可用。
			rollback, err := server.Definitions().RegisterWorkflowInNamespace(namespace, contentA)
			if err != nil {
				t.Fatalf("rollback: %v", err)
			}
			assertActiveVersion(t, server, namespace, "enroll-flow", rollback.Version)

			// 坏配置：校验失败，latest 停在回滚后的版本。
			bad := &program.WorkflowConfig{ID: "enroll-flow", Name: "bad", RunTimeout: "10m", ExecutionTimeout: "1m", Program: programtest.Noop()}
			if _, err := server.Definitions().EnrollWorkflowInNamespace(namespace, bad); err == nil {
				t.Fatal("非法配置应被拒绝")
			}
			assertActiveVersion(t, server, namespace, "enroll-flow", rollback.Version)
		})
	}
}

// TestEnrollEmptyLatest 覆盖「先只登记、
// 之后首次 startup 时 latest 还是空的」场景：同内容 enroll 应补激活。
func TestEnrollEmptyLatest(t *testing.T) {
	server := newTestServer()
	const namespace = "artifact-only"
	workflow := &program.WorkflowConfig{ID: "artifact-only-flow", Name: "A", Program: programtest.Noop()}

	registered, err := server.Definitions().RegisterWorkflowArtifactInNamespace(namespace, workflow)
	if err != nil {
		t.Fatalf("register artifact: %v", err)
	}
	if _, err := server.activeWorkflowRefInNamespace(namespace, "artifact-only-flow"); err == nil {
		t.Fatal("只登记后不应该有 latest")
	}

	enrolled, err := server.Definitions().EnrollWorkflowInNamespace(namespace, workflow)
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	if enrolled.Version != registered.Version {
		t.Fatalf("enroll 版本 = %q, want %q", enrolled.Version, registered.Version)
	}
	assertActiveVersion(t, server, namespace, "artifact-only-flow", registered.Version)
}

func assertActiveVersion(t *testing.T, server *Server, namespace, workflowID, want string) {
	t.Helper()
	active, err := server.activeWorkflowRefInNamespace(namespace, workflowID)
	if err != nil {
		t.Fatalf("读取 %s/%s latest: %v", namespace, workflowID, err)
	}
	if active.Version != want {
		t.Fatalf("%s/%s latest = %q, want %q", namespace, workflowID, active.Version, want)
	}
	if strings.TrimSpace(active.Version) == "" {
		t.Fatal("latest 版本为空")
	}
}
