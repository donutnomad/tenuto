package definitions_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/usecase/definitions"
	"github.com/donutnomad/tenuto/server/infra/memory"
	"github.com/donutnomad/tenuto/server/infra/systemclock"
)

// ============================================
// definitions 用例测试
// ============================================
// 测试策略:
// - 仓储替身用 infra/memory，版本号与内容 hash 走真实路径
// - Activity 路由目录与活跃 Worker 用本文件的 fake，记录调用顺序
// ============================================

// fakeRoutes 记录刷新与校验的调用，并按配置返回错误。
type fakeRoutes struct {
	refreshErr  error
	validateErr error
	calls       []string
}

func (r *fakeRoutes) RefreshActivityRoutes(time.Time) error {
	r.calls = append(r.calls, "refresh")
	return r.refreshErr
}

func (r *fakeRoutes) ValidateWorkflowConfigInNamespace(namespace string, workflow *programpkg.WorkflowConfig) error {
	r.calls = append(r.calls, "validate:"+namespace+"/"+workflow.Type)
	return r.validateErr
}

type fakeWorkers struct {
	workers []protocol.ActivityWorkerVersion
	err     error
	calls   int
}

func (w *fakeWorkers) ListActiveActivityWorkerVersions(time.Time) ([]protocol.ActivityWorkerVersion, error) {
	w.calls++
	return w.workers, w.err
}

type testEnv struct {
	store   *memory.DefinitionRepository
	routes  *fakeRoutes
	workers *fakeWorkers
	usecase *definitions.UseCase
}

func newEnv() *testEnv {
	env := &testEnv{store: memory.NewDefinitionRepository(), routes: &fakeRoutes{}, workers: &fakeWorkers{}}
	env.usecase = definitions.NewUseCase(env.store, env.routes, env.workers, systemclock.Clock{})
	return env
}

func TestRegisterWorkflowVersionsPerNamespace(t *testing.T) {
	// Happy Path: 同一 workflow ID 在两个 namespace 各有自己的内容版本
	env := newEnv()
	first, err := env.usecase.RegisterWorkflowInNamespace("tenant-a", &programpkg.WorkflowConfig{Type: "payments", Name: "Payments A", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	second, err := env.usecase.RegisterWorkflowInNamespace("tenant-b", &programpkg.WorkflowConfig{Type: "payments", Name: "Payments B", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	if first.Namespace == second.Namespace || first.WorkflowID != second.WorkflowID {
		t.Fatalf("refs do not represent separate tenant definitions: first=%+v second=%+v", first, second)
	}
	firstConfig, err := env.store.GetWorkflowConfig(first)
	if err != nil {
		t.Fatal(err)
	}
	secondConfig, err := env.store.GetWorkflowConfig(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstConfig.Name != "Payments A" || secondConfig.Name != "Payments B" {
		t.Fatalf("namespace configs crossed: first=%q second=%q", firstConfig.Name, secondConfig.Name)
	}
}

func TestRegisterWorkflowRefreshesRoutesThenValidatesBeforePersisting(t *testing.T) {
	// Error Case: 路由校验失败时配置不保存；刷新先于校验
	env := newEnv()
	wantErr := errors.New("route unavailable")
	env.routes.validateErr = wantErr

	if _, err := env.usecase.RegisterWorkflowInNamespace("tenant-a", &programpkg.WorkflowConfig{Type: "payments", Program: programtest.Noop()}); !errors.Is(err, wantErr) {
		t.Fatalf("register error = %v, want %v", err, wantErr)
	}
	if _, err := env.store.LatestWorkflowConfigRefInNamespace("tenant-a", "payments"); err == nil {
		t.Fatal("workflow was persisted after route validation failed")
	}
	if got := strings.Join(env.routes.calls, ","); got != "refresh,validate:tenant-a/payments" {
		t.Fatalf("route calls = %q", got)
	}
}

func TestRegisterWorkflowStopsOnRefreshError(t *testing.T) {
	// Error Case: 路由目录刷新失败时注册与登记都直接返回该错误
	wantErr := errors.New("worker versions unavailable")
	for name, register := range map[string]func(*definitions.UseCase) error{
		"register": func(u *definitions.UseCase) error {
			_, err := u.RegisterWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", Program: programtest.Noop()})
			return err
		},
		"enroll": func(u *definitions.UseCase) error {
			_, err := u.EnrollWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", Program: programtest.Noop()})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			env := newEnv()
			env.routes.refreshErr = wantErr
			if err := register(env.usecase); !errors.Is(err, wantErr) {
				t.Fatalf("error = %v, want %v", err, wantErr)
			}
			if refs, err := env.store.ListWorkflowConfigRefsInNamespace("ns"); err != nil || len(refs) != 0 {
				t.Fatalf("content stored after refresh failure: %+v", refs)
			}
		})
	}
}

func TestRegisterWorkflowValidatesConfig(t *testing.T) {
	// Error Case: 静态校验失败的配置不触发路由校验，也不保存
	for name, workflow := range map[string]*programpkg.WorkflowConfig{
		"nil config":                            nil,
		"empty id":                              {Name: "no id"},
		"missing program":                       {Type: "payments"},
		"invalid program":                       {Type: "payments", Program: programpkg.WLangProgram{Schema: "wlang/program/v1", Program: []any{map[string]any{"return": nil}}}},
		"run timeout exceeds execution timeout": {Type: "payments", Program: programtest.Noop(), RunTimeout: "2h", ExecutionTimeout: "1h"},
	} {
		t.Run(name, func(t *testing.T) {
			env := newEnv()
			if _, err := env.usecase.RegisterWorkflowInNamespace("ns", workflow); err == nil {
				t.Fatal("invalid config was registered")
			}
			if _, err := env.usecase.RegisterWorkflowArtifactInNamespace("ns", workflow); err == nil {
				t.Fatal("invalid config was registered as artifact")
			}
			if _, err := env.usecase.EnrollWorkflowInNamespace("ns", workflow); err == nil {
				t.Fatal("invalid config was enrolled")
			}
			for _, call := range env.routes.calls {
				if strings.HasPrefix(call, "validate:") {
					t.Fatalf("route validation ran for an invalid config: %v", env.routes.calls)
				}
			}
			if refs, err := env.store.ListWorkflowConfigRefsInNamespace("ns"); err != nil || len(refs) != 0 {
				t.Fatalf("invalid config stored: %+v", refs)
			}
		})
	}
}

func TestRegisterWorkflowTargetWorker(t *testing.T) {
	online := []protocol.ActivityWorkerVersion{{Namespace: "ns", Identity: "worker-a"}, {Namespace: "", Identity: "worker-default"}}

	t.Run("online target in namespace", func(t *testing.T) {
		// Happy Path: 目标 Worker 在该 namespace 在线时注册成功，配置保留目标
		env := newEnv()
		env.workers.workers = online
		ref, err := env.usecase.RegisterWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", TargetWorkerIdentity: "worker-a", Program: programtest.Noop()})
		if err != nil {
			t.Fatal(err)
		}
		stored, err := env.store.GetWorkflowConfig(ref)
		if err != nil || stored.TargetWorkerIdentity != "worker-a" {
			t.Fatalf("stored config = %+v err=%v", stored, err)
		}
	})
	t.Run("empty namespace matches default", func(t *testing.T) {
		// Edge Case: Worker 的空 namespace 归入 default
		env := newEnv()
		env.workers.workers = online
		if _, err := env.usecase.RegisterWorkflowInNamespace("", &programpkg.WorkflowConfig{Type: "payments", TargetWorkerIdentity: "worker-default", Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("target in another namespace", func(t *testing.T) {
		// Error Case: 其他 namespace 的同名 Worker 不算在线
		env := newEnv()
		env.workers.workers = online
		_, err := env.usecase.RegisterWorkflowInNamespace("other", &programpkg.WorkflowConfig{Type: "payments", TargetWorkerIdentity: "worker-a", Program: programtest.Noop()})
		if err == nil || err.Error() != `target worker "worker-a" is offline or unknown in namespace "other"` {
			t.Fatalf("error = %v", err)
		}
		if refs, err := env.store.ListWorkflowConfigRefsInNamespace("other"); err != nil || len(refs) != 0 {
			t.Fatalf("config stored for an offline target: %+v", refs)
		}
	})
	t.Run("identity too long", func(t *testing.T) {
		// Error Case: 超过 191 字符的目标在读取 Worker 列表之前被拒绝
		env := newEnv()
		_, err := env.usecase.RegisterWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", TargetWorkerIdentity: strings.Repeat("w", 192), Program: programtest.Noop()})
		if err == nil || err.Error() != "target_worker_identity exceeds 191 characters" {
			t.Fatalf("error = %v", err)
		}
		if env.workers.calls != 0 {
			t.Fatalf("worker list read %d times for an over-long identity", env.workers.calls)
		}
	})
	t.Run("worker list error", func(t *testing.T) {
		// Error Case: 读取 Worker 列表失败时返回该错误
		env := newEnv()
		env.workers.err = errors.New("worker store unavailable")
		if _, err := env.usecase.RegisterWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", TargetWorkerIdentity: "worker-a", Program: programtest.Noop()}); !errors.Is(err, env.workers.err) {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("no target skips worker lookup", func(t *testing.T) {
		// Edge Case: 未指定目标时不读取 Worker 列表
		env := newEnv()
		if _, err := env.usecase.RegisterWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", Program: programtest.Noop()}); err != nil {
			t.Fatal(err)
		}
		if env.workers.calls != 0 {
			t.Fatalf("worker list read %d times without a target", env.workers.calls)
		}
	})
}

func TestRegisterWorkflowArtifactKeepsActive(t *testing.T) {
	// Happy Path: artifact 登记保存内容版本，active 留在原版本，且不刷新也不校验路由
	env := newEnv()
	active, err := env.usecase.RegisterWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", Name: "A", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	env.routes.calls = nil
	env.routes.validateErr = errors.New("route unavailable")
	artifact, err := env.usecase.RegisterWorkflowArtifactInNamespace(" ns ", &programpkg.WorkflowConfig{Type: "payments", Name: "B", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Namespace != "ns" || artifact.Version == active.Version {
		t.Fatalf("artifact ref = %+v", artifact)
	}
	if latest, err := env.store.LatestWorkflowConfigRefInNamespace("ns", "payments"); err != nil || latest != active {
		t.Fatalf("active = %+v err=%v, want %+v", latest, err, active)
	}
	if len(env.routes.calls) != 0 {
		t.Fatalf("artifact registration touched routes: %v", env.routes.calls)
	}
}

// TestEnrollRouteFailureKeepsLatest 锁住「路由校验失败不改 latest」：
// Enroll 与 Register 共用同一个路由校验，失败必须发生在写 latest 之前。
func TestEnrollRouteFailureKeepsLatest(t *testing.T) {
	env := newEnv()
	first, err := env.usecase.EnrollWorkflowInNamespace("tenant", &programpkg.WorkflowConfig{Type: "payments", Name: "A", Program: programtest.Noop()})
	if err != nil {
		t.Fatalf("enroll A: %v", err)
	}
	env.routes.validateErr = errors.New("route unavailable")
	if _, err := env.usecase.EnrollWorkflowInNamespace("tenant", &programpkg.WorkflowConfig{Type: "payments", Name: "B", Program: programtest.Noop()}); err == nil {
		t.Fatal("路由校验失败时应拒绝登记")
	}
	active, err := env.store.LatestWorkflowConfigRefInNamespace("tenant", "payments")
	if err != nil {
		t.Fatalf("读取 latest: %v", err)
	}
	if active.Version != first.Version {
		t.Fatalf("latest = %q, want %q", active.Version, first.Version)
	}
}

// TestReplaySkipsRouteCheck 锁住启动可用性边界：已存在内容 + 已有 latest 的重复登记
// 只做静态校验，不因 Activity 路由缺失失败；新内容仍需路由校验。
func TestReplaySkipsRouteCheck(t *testing.T) {
	env := newEnv()
	content := &programpkg.WorkflowConfig{Type: "payments", Name: "A", Program: programtest.Noop()}
	first, err := env.usecase.EnrollWorkflowInNamespace("tenant", content)
	if err != nil {
		t.Fatalf("首次登记: %v", err)
	}
	env.routes.validateErr = errors.New("route unavailable")
	env.routes.calls = nil
	replay, err := env.usecase.EnrollWorkflowInNamespace("tenant", content)
	if err != nil {
		t.Fatalf("同内容重复登记不应做路由校验: %v", err)
	}
	if replay.Version != first.Version {
		t.Fatalf("重复登记版本 = %q, want %q", replay.Version, first.Version)
	}
	if got := strings.Join(env.routes.calls, ","); got != "refresh" {
		t.Fatalf("重复登记的路由调用 = %q, want 只有 refresh", got)
	}
	if _, err := env.usecase.EnrollWorkflowInNamespace("tenant", &programpkg.WorkflowConfig{Type: "payments", Name: "B", Program: programtest.Noop()}); err == nil {
		t.Fatal("新内容应被路由校验拒绝")
	}
	active, err := env.store.LatestWorkflowConfigRefInNamespace("tenant", "payments")
	if err != nil {
		t.Fatalf("读取 latest: %v", err)
	}
	if active.Version != first.Version {
		t.Fatalf("latest = %q, want %q", active.Version, first.Version)
	}
}

func TestEnrollRejectsOverlongTargetBeforeRefresh(t *testing.T) {
	// Error Case: 目标超长时在刷新路由之前返回
	env := newEnv()
	_, err := env.usecase.EnrollWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", TargetWorkerIdentity: strings.Repeat("w", 192), Program: programtest.Noop()})
	if err == nil || err.Error() != "target_worker_identity exceeds 191 characters" {
		t.Fatalf("error = %v", err)
	}
	if len(env.routes.calls) != 0 {
		t.Fatalf("routes touched before the length check: %v", env.routes.calls)
	}
}

// TestReplicasShareLatestVersion 锁住多副本一致性：一个副本激活新版本后，
// 共享同一存储的其他副本立刻读到它，包括此前已经读过旧版本的副本。
func TestReplicasShareLatestVersion(t *testing.T) {
	store := memory.NewDefinitionRepository()
	newReplica := func() *definitions.UseCase {
		return definitions.NewUseCase(store, &fakeRoutes{}, &fakeWorkers{}, systemclock.Clock{})
	}
	replicaA, replicaB := newReplica(), newReplica()

	first, err := replicaA.RegisterWorkflowInNamespace("tenant", &programpkg.WorkflowConfig{Type: "payments", Name: "A", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	if ref, err := definition.ActiveWorkflowRef(store, "tenant", "payments"); err != nil || ref != first {
		t.Fatalf("active = %+v err=%v, want %+v", ref, err, first)
	}

	second, err := replicaB.EnrollWorkflowInNamespace("tenant", &programpkg.WorkflowConfig{Type: "payments", Name: "B", Program: programtest.Noop()})
	if err != nil {
		t.Fatal(err)
	}
	if second.Version == first.Version {
		t.Fatal("new content must produce a new version")
	}
	if ref, err := definition.ActiveWorkflowRef(store, "tenant", "payments"); err != nil || ref != second {
		t.Fatalf("active = %+v err=%v, want %+v", ref, err, second)
	}
	// 副本 A 之后发布指向 Worker 的版本时，以副本 B 激活的内容为基础。
	targeted, err := replicaA.SetWorkflowWorkerTarget("tenant", "payments", "")
	if err != nil {
		t.Fatal(err)
	}
	if targeted != second {
		t.Fatalf("replica A republished %+v, want the version activated by replica B %+v", targeted, second)
	}
}

func TestRegisterWorkflowJSON(t *testing.T) {
	t.Run("enrolls decoded artifact", func(t *testing.T) {
		// Happy Path: 返回内容版本，并激活首次出现的内容
		env := newEnv()
		workflow := &programpkg.WorkflowConfig{Type: "payments", Name: "A", Program: programtest.Noop()}
		data, err := json.Marshal(workflow)
		if err != nil {
			t.Fatal(err)
		}
		version, err := env.usecase.RegisterWorkflowJSON(context.Background(), "ns", data)
		if err != nil {
			t.Fatal(err)
		}
		want, err := programpkg.WorkflowConfigVersion(workflow)
		if err != nil {
			t.Fatal(err)
		}
		if version != want {
			t.Fatalf("version = %q, want content hash %q", version, want)
		}
		if latest, err := env.store.LatestWorkflowConfigRefInNamespace("ns", "payments"); err != nil || latest.Version != version {
			t.Fatalf("active = %+v err=%v, want version %q", latest, err, version)
		}
	})
	t.Run("invalid json", func(t *testing.T) {
		// Error Case: 解码失败
		env := newEnv()
		_, err := env.usecase.RegisterWorkflowJSON(context.Background(), "ns", []byte(`{`))
		if err == nil || !strings.HasPrefix(err.Error(), "decode workflow artifact: ") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("cancelled context", func(t *testing.T) {
		// Error Case: ctx 已取消时不登记
		env := newEnv()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := env.usecase.RegisterWorkflowJSON(ctx, "ns", []byte(`{"type":"payments"}`)); !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
		if refs, err := env.store.ListWorkflowConfigRefsInNamespace("ns"); err != nil || len(refs) != 0 {
			t.Fatalf("content stored with a cancelled context: %+v", refs)
		}
	})
}

func TestSetWorkflowWorkerTarget(t *testing.T) {
	t.Run("publishes a new version with the target", func(t *testing.T) {
		// Happy Path: 以 active 内容为基础发布带目标的新版本；原版本内容保持原样
		env := newEnv()
		env.workers.workers = []protocol.ActivityWorkerVersion{{Namespace: "ns", Identity: "worker-a"}}
		original, err := env.usecase.RegisterWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", Name: "A", Program: programtest.Noop()})
		if err != nil {
			t.Fatal(err)
		}
		targeted, err := env.usecase.SetWorkflowWorkerTarget("ns", "payments", "worker-a")
		if err != nil {
			t.Fatal(err)
		}
		if targeted.Version == original.Version {
			t.Fatal("setting a target must publish a new content version")
		}
		if active, err := definition.ActiveWorkflowRef(env.store, "ns", "payments"); err != nil || active != targeted {
			t.Fatalf("active = %+v err=%v, want %+v", active, err, targeted)
		}
		stored, err := env.store.GetWorkflowConfig(targeted)
		if err != nil || stored.TargetWorkerIdentity != "worker-a" || stored.Name != "A" {
			t.Fatalf("targeted config = %+v err=%v", stored, err)
		}
		untouched, err := env.store.GetWorkflowConfig(original)
		if err != nil || untouched.TargetWorkerIdentity != "" {
			t.Fatalf("original config = %+v err=%v", untouched, err)
		}

		// 清空目标回到原内容版本。
		cleared, err := env.usecase.SetWorkflowWorkerTarget("ns", "payments", "")
		if err != nil || cleared != original {
			t.Fatalf("cleared = %+v err=%v, want %+v", cleared, err, original)
		}
	})
	t.Run("offline target", func(t *testing.T) {
		// Error Case: 目标离线时 active 保持原版本
		env := newEnv()
		original, err := env.usecase.RegisterWorkflowInNamespace("ns", &programpkg.WorkflowConfig{Type: "payments", Program: programtest.Noop()})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := env.usecase.SetWorkflowWorkerTarget("ns", "payments", "missing"); err == nil || !strings.Contains(err.Error(), "offline or unknown") {
			t.Fatalf("error = %v", err)
		}
		if active, err := definition.ActiveWorkflowRef(env.store, "ns", "payments"); err != nil || active != original {
			t.Fatalf("active = %+v err=%v, want %+v", active, err, original)
		}
	})
	t.Run("unregistered workflow", func(t *testing.T) {
		// Error Case: workflow 没有 active 版本
		env := newEnv()
		_, err := env.usecase.SetWorkflowWorkerTarget("ns", "missing", "worker-a")
		if !errors.Is(err, definition.ErrWorkflowConfigNotFound) || !strings.HasPrefix(err.Error(), "workflow ns/missing has no registered config version") {
			t.Fatalf("error = %v", err)
		}
	})
}
