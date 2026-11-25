package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/service/engine"
	"github.com/donutnomad/tenuto/server/infra/memory"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
)

// 本文件是 UNKNOWN-04 的可执行基准：Workflow 内容 A 已推进到 c 完成，发布内容 B，
// B 修改“已完成 Activity c”的声明契约，然后继续用同一条老 Run 的 history replay。
//
// 夹具走推荐生成路径：@Activity/@Workflow 注解 → sdkgen.Scan → 写生成桥 → sdkgen.Generate。
// 因此 ActivityRef 的 Input/Output 来自注解与真实 wire 指纹，不是手工拼的字符串。
// 解释器 replay、契约校验、路由、事件写入全部走生产代码。
//
// 两个子用例只改变 B 中 c 的声明：
//   - input-change：c 的输入新增必填字段
//   - output-change：c 的输出字段 int64 → string，History 里 c 的结果读不回：升级前的载荷检查拒绝换版，Run 留在 A
//     以 ConfigError 暂停

type contractFixture struct {
	root       string
	workflowID string
}

const (
	contractCInDefault    = "Amount int64 `json:\"amount\"`"
	contractCOutInt       = "Amount int64 `json:\"amount\"`"
	contractCOutString    = "Amount string `json:\"amount\"`"
	contractCInWithNote   = "Amount int64 `json:\"amount\"`\n\tNote   string `json:\"note\"`"
	contractDInDefault    = "Receipt any `json:\"receipt\"`"
	contractDInWithNote   = "Receipt any `json:\"receipt\"`\n\tNote    string `json:\"note\"`"
	contractDefaultSteps  = "b,c,d"
	contractDefaultTimout = 10 * time.Second
)

// newContractFixture 在仓库模块内建临时注解夹具并生成真实 program。
func newContractFixture(t *testing.T, name, version string, cIn, cOut string) contractFixture {
	t.Helper()
	base, err := filepath.Abs("testdata")
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(base, "tmp-contract-replay-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	moduleRoot, modulePath := moduleRootOf(t)
	rel, err := filepath.Rel(moduleRoot, root)
	if err != nil {
		t.Fatal(err)
	}
	actionsImport := modulePath + "/" + filepath.ToSlash(filepath.Join(rel, "activities"))
	workflowID := "contract-replay-" + name

	if err := os.MkdirAll(filepath.Join(root, "activities"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "flows"), 0o755); err != nil {
		t.Fatal(err)
	}
	activitiesSrc := fmt.Sprintf(`package activities

import "context"

type BIn struct{}
type BOut struct{ Value int64 `+"`json:\"value\"`"+` }

type CIn struct{ %s }
type COut struct{ %s }

type DIn struct{ Receipt any `+"`json:\"receipt\"`"+` }
type DOut struct{}

type EIn struct{ Receipt any `+"`json:\"receipt\"`"+` }
type EOut struct{}

// @Activity(type=example.b,queue=contract-replay)
func B(_ context.Context, _ BIn) (BOut, error) { return BOut{}, nil }

// @Activity(type=example.c,queue=contract-replay)
func C(_ context.Context, _ CIn) (COut, error) { return COut{}, nil }

// @Activity(type=example.d,queue=contract-replay)
func D(_ context.Context, _ DIn) (DOut, error) { return DOut{}, nil }

// @Activity(type=example.e,queue=contract-replay)
func E(_ context.Context, _ EIn) (EOut, error) { return EOut{}, nil }
`, cIn, cOut)
	if err := os.WriteFile(filepath.Join(root, "activities", "activities.go"), []byte(activitiesSrc), 0o644); err != nil {
		t.Fatal(err)
	}

	flowSrc := fmt.Sprintf(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	%q
)

// @Workflow(type=%s,name=%s,policy=auto_upgrade,version=%s)
func Flow(ctx workflow.Context) error {
	if _, err := activities.CallB(ctx, activities.BIn{}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}); err != nil {
		return err
	}
	cOut, err := activities.CallC(ctx, activities.CIn{Amount: 42}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return err
	}
	if _, err := activities.CallD(ctx, activities.DIn{Receipt: cOut}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}); err != nil {
		return err
	}
	if _, err := activities.CallE(ctx, activities.EIn{Receipt: cOut}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}); err != nil {
		return err
	}
	return nil
}
`, actionsImport, workflowID, workflowID, version)
	flowPath := filepath.Join(root, "flows", "flow.go")
	if err := os.WriteFile(flowPath, []byte(flowSrc), 0o644); err != nil {
		t.Fatal(err)
	}

	plan, err := sdkgen.Scan(filepath.Join(root, "flows"))
	if err != nil {
		t.Fatalf("sdkgen.Scan: %v", err)
	}
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sdkgen.Generate(plan); err != nil {
		t.Fatalf("sdkgen.Generate: %v", err)
	}
	return contractFixture{root: root, workflowID: workflowID}
}

// generate 重新生成当前夹具内容，返回完整 WorkflowConfig（供断言声明元数据）。
func (f contractFixture) generate(t *testing.T) *program.WorkflowConfig {
	t.Helper()
	plan, err := sdkgen.Scan(filepath.Join(f.root, "flows"))
	if err != nil {
		t.Fatalf("sdkgen.Scan: %v", err)
	}
	for _, bridge := range plan.Bridges {
		if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	result, err := sdkgen.Generate(plan)
	if err != nil {
		t.Fatalf("sdkgen.Generate: %v", err)
	}
	return result.Workflow
}

// register 生成并注册当前夹具内容。
func (f contractFixture) register(t *testing.T, server *engine.Server) definition.WorkflowConfigRef {
	t.Helper()
	ref, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, f.generate(t))
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return ref
}

func moduleRootOf(t *testing.T) (string, string) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		data, readErr := os.ReadFile(filepath.Join(dir, "go.mod"))
		if readErr == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
					return dir, strings.TrimSpace(rest)
				}
			}
			t.Fatal("go.mod has no module line")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

// contractWorker 用真实 fingerprint 算法上报 A 的声明契约。
type contractWorkerIn struct{}
type contractWorkerOut struct {
	Value int64 `json:"value"`
}
type contractWorkerCIn struct {
	Amount int64 `json:"amount"`
}
type contractWorkerCOut struct {
	Amount int64 `json:"amount"`
}
type contractWorkerDIn struct {
	Receipt any `json:"receipt"`
}
type contractWorkerDOut struct{}
type contractWorkerEIn struct {
	Receipt any `json:"receipt"`
}
type contractWorkerEOut struct{}

// contractWorkerCInWithNote 与 contractWorkerDInWithNote 是 c、d 的输入新增 note 字段之后的结构。
type contractWorkerCInWithNote struct {
	Amount int64  `json:"amount"`
	Note   string `json:"note"`
}
type contractWorkerDInWithNote struct {
	Receipt any    `json:"receipt"`
	Note    string `json:"note"`
}

// registerContractWorker 上报 A 的 b、c、d、e 契约。
func registerContractWorker(t *testing.T, server *engine.Server) {
	t.Helper()
	registerContractWorkerReporting(t, server, false, false)
}

func TestCompletedActivityContractChangesOnReplay(t *testing.T) {
	cases := []struct {
		name  string
		bCIn  string
		bCOut string
		// wantPayloadPause 表示 History 里 c 的结果读不回 B 的新 DTO：换版前的载荷检查拒绝升级，
		// Run 留在 A 并以 ConfigError 暂停，不会在 B 上解码失败。
		wantPayloadPause bool
	}{
		{
			name:  "input-change",
			bCIn:  contractCInWithNote,
			bCOut: contractCOutInt,
		},
		{
			name:             "output-change",
			bCIn:             contractCInDefault,
			bCOut:            contractCOutString,
			wantPayloadPause: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, mgr, activities := newUpgradeReplayServer(t)
			ctx := context.Background()

			fixtureA := newContractFixture(t, tc.name+"-a", "v1", contractCInDefault, contractCOutInt)
			registerContractWorker(t, server)
			refA := fixtureA.register(t, server)
			runID, err := startedRunID(server.StartWorkflow(ctx, "", fixtureA.workflowID, fixtureA.workflowID, "", nil))
			if err != nil {
				t.Fatal(err)
			}

			// a 场景里 b、c 依次完成；c 完成后 d 已调度。
			completeByType(t, server, activities, "example.b")
			completeByTypeWithOutput(t, server, activities, "example.c", map[string]any{"amount": 42})
			waitForScheduledType(t, mgr, runID, "example.d", 1)
			assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "before publish")

			// 发布 B：用同一夹具目录改写 activities 注解并重新生成。
			writeContractActivities(t, fixtureA.root, tc.bCIn, tc.bCOut, contractDInDefault)
			fixtureB := fixtureA
			refB := fixtureB.register(t, server)
			if refB.Version == refA.Version {
				t.Fatal("B content version must differ from A")
			}
			// 发布本身不是 WFT 边界。
			assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "publish must not move current")

			// d 在 A 下已调度，其 Input 是 A 的旧快照；完成 d 触发 B 的边界 WFT。
			completeByType(t, server, activities, "example.d")

			if tc.wantPayloadPause {
				waitForRunConfigErrorForTest(t, mgr, runID)
				run, err := mgr.Get(runID)
				if err != nil {
					t.Fatal(err)
				}
				want := `result of activity "example.c": activities.COut -> activities.COut: amount: type changed from int64 to string`
				if !strings.Contains(run.ConfigError, want) || run.Status != historypkg.StatusRunning {
					t.Fatalf("run status %s config error %q, want it to contain %q", run.Status, run.ConfigError, want)
				}
				history := mgrEvents(t, mgr, runID)
				if failures := workflowFailures(history); len(failures) != 0 {
					t.Fatalf("workflow failed: %v", failures)
				}
				if got := countScheduledType(history, "example.e"); got != 0 {
					t.Fatalf("paused run scheduled e %d times", got)
				}
				// 暂停时没有 Workflow Task 在 B 上开始，current 仍是 A。
				assertRunVersion(t, mgr, runID, refA.Version, refA.Version, "paused before B")
				return
			}

			// e 由 B 的边界 WFT 首次调度：它的输入就是 B 对旧 c 结果的解码结果。
			waitForScheduledType(t, mgr, runID, "example.e", 1)
			eInput := completeByTypeRecordingInput(t, server, activities, "example.e")
			if amount := replayReceiptAmount(t, eInput); amount != "42" {
				t.Fatalf("B decoded old c result as %s, want the JSON number 42", amount)
			}

			waitForRunStatusWithin(t, server, runID, historypkg.StatusCompleted, 20*time.Second)
			run, err := mgr.Get(runID)
			if err != nil {
				t.Fatal(err)
			}
			if run.ConfigError != "" || run.UpgradeError != "" {
				t.Fatalf("compatible replay must not record errors: config=%q upgrade=%q", run.ConfigError, run.UpgradeError)
			}
			assertRunVersion(t, mgr, runID, refB.Version, refA.Version, "completed run")

			history := mgrEvents(t, mgr, runID)
			if got := countScheduledType(history, "example.c"); got != 1 {
				t.Fatalf("completed c scheduled %d times, want 1 (result must be reused)", got)
			}
			if got := countScheduledType(history, "example.d"); got != 1 {
				t.Fatalf("d scheduled %d times, want 1", got)
			}
			// e 首次调度发生在 B 的边界 WFT，其输入是 B 对旧 c 结果的解码。
			if got := countScheduledType(history, "example.e"); got != 1 {
				t.Fatalf("e scheduled %d times, want 1", got)
			}
			if got := countEventType(history, "WorkflowTaskFailed"); got != 0 {
				t.Fatalf("replay recorded %d WFT failures, want 0", got)
			}
		})
	}
}

func writeContractActivities(t *testing.T, root, cIn, cOut, dIn string) {
	t.Helper()
	path := filepath.Join(root, "activities", "activities.go")
	src := fmt.Sprintf(`package activities

import "context"

type BIn struct{}
type BOut struct{ Value int64 `+"`json:\"value\"`"+` }

type CIn struct{ %s }
type COut struct{ %s }

type DIn struct{ %s }
type DOut struct{}

type EIn struct{ Receipt any `+"`json:\"receipt\"`"+` }
type EOut struct{}

// @Activity(type=example.b,queue=contract-replay)
func B(_ context.Context, _ BIn) (BOut, error) { return BOut{}, nil }

// @Activity(type=example.c,queue=contract-replay)
func C(_ context.Context, _ CIn) (COut, error) { return COut{}, nil }

// @Activity(type=example.d,queue=contract-replay)
func D(_ context.Context, _ DIn) (DOut, error) { return DOut{}, nil }

// @Activity(type=example.e,queue=contract-replay)
func E(_ context.Context, _ EIn) (EOut, error) { return EOut{}, nil }
`, cIn, cOut, dIn)
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
}

// replayReceiptAmount 从 e 的输入里取 c 输出在 B 下解码、再按类型表编码进载荷的 amount（JSON 数字原文）。
func replayReceiptAmount(t *testing.T, input map[string]any) string {
	t.Helper()
	receipt, ok := input["receipt"].(map[string]any)
	if !ok {
		t.Fatalf("e input receipt = %T, want a JSON object", input["receipt"])
	}
	amount, ok := receipt["amount"].(json.Number)
	if !ok {
		t.Fatalf("e input receipt amount = %#v, want a JSON number", receipt["amount"])
	}
	return amount.String()
}

func completeByTypeWithOutput(t *testing.T, server *engine.Server, queue *memory.TaskQueue, activityType string, output map[string]any) {
	t.Helper()
	task := dequeueActivityTaskWithin(t, queue, activityType, 10*time.Second)
	if err := server.HandleActivityCompleted(context.Background(), task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, output); err != nil {
		t.Fatal(err)
	}
}

func completeByTypeRecordingInput(t *testing.T, server *engine.Server, queue *memory.TaskQueue, activityType string) map[string]any {
	t.Helper()
	task := dequeueActivityTaskWithin(t, queue, activityType, 10*time.Second)
	input := task.Input
	if err := server.HandleActivityCompleted(context.Background(), task.RunID, task.ScheduledEventID, task.Attempt, task.Stamp, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	return input
}

// workflowFailures 收集 Workflow 失败与 Workflow Task 失败。被新事实取代后重做的 Workflow Task
// （cause unhandled_events）是正常调度结果，不计入。
func workflowFailures(events []historypkg.Event) []string {
	var out []string
	for _, event := range events {
		if event.WorkflowTaskFailed != nil && event.WorkflowTaskFailed.Cause != execution.WorkflowTaskFailedCauseUnhandledEvents {
			out = append(out, "wft: "+event.WorkflowTaskFailed.Cause+": "+event.WorkflowTaskFailed.Reason)
		}
		if event.WorkflowFailed != nil {
			out = append(out, "workflow: "+event.WorkflowFailed.Error)
		}
	}
	return out
}

// b 已完成、c 尚未调度的发布窗口，分开验证两个边界：
//  1. 承担内容切换的边界 WFT 里，Activity 契约对照读本次选中内容（B）的声明；
//  2. 切换提交后的下一个 WFT 继续用 current（B）的声明做对照。
func TestPendingActivityContractWindow(t *testing.T) {
	prepare := func(t *testing.T, name string) (*replayServerControl, contractFixture, definition.WorkflowConfigRef, string) {
		t.Helper()
		controlled := newControlledReplayServer(t)
		registerContractWorker(t, controlled.Server)
		fixture := newContractFixture(t, name, "v1", contractCInDefault, contractCOutInt)
		refA := fixture.register(t, controlled.Server)
		controlled.StartWorker(t)
		runID, err := startedRunID(controlled.Server.StartWorkflow(context.Background(), "", fixture.workflowID, fixture.workflowID, "", nil))
		if err != nil {
			t.Fatal(err)
		}
		waitForScheduledType(t, controlled.Mgr, runID, "example.b", 1)
		controlled.StopWorker()
		completeContractActivity(t, controlled.Server, controlled.Activities, "example.b")
		if got := countScheduledType(mgrEvents(t, controlled.Mgr, runID), "example.c"); got != 0 {
			t.Fatalf("c scheduled %d times before publish, want 0", got)
		}
		return controlled, fixture, refA, runID
	}

	t.Run("promotion-wft-uses-candidate-declaration", func(t *testing.T) {
		controlled, fixture, refA, runID := prepare(t, "pending-window-promotion")

		wfA := fixture.generate(t)
		writeContractActivities(t, fixture.root, contractCInWithNote, contractCOutInt, contractDInDefault)
		wfB := fixture.generate(t)
		if declaredRefForTest(t, wfB, "example.c").Input == declaredRefForTest(t, wfA, "example.c").Input {
			t.Fatal("B must declare a different input for c")
		}
		refB := fixture.register(t, controlled.Server)
		if refB.Version == refA.Version {
			t.Fatal("B content version must differ from A")
		}
		controlled.StartWorker(t)

		// 切换 WFT 按候选内容 B 对 c 的新输入声明对照只上报旧输入的 worker：写 ConfigError，c 不派发。
		waitForRunConfigErrorForTest(t, controlled.Mgr, runID)
		if got := countScheduledType(mgrEvents(t, controlled.Mgr, runID), "example.c"); got != 0 {
			t.Fatalf("promotion WFT scheduled c %d times against B's new declaration, want 0", got)
		}
	})

	t.Run("next-wft-enforces-new-declaration", func(t *testing.T) {
		controlled, fixture, refA, runID := prepare(t, "pending-window-next")
		writeContractActivities(t, fixture.root, contractCInDefault, contractCOutInt, contractDInWithNote)
		refB := fixture.register(t, controlled.Server)
		controlled.StartWorker(t)

		// c 在切换 WFT 内按旧声明调度；完成后下一个 WFT 用 B 对 d 的新输入声明对照只上报旧输入的 worker。
		waitForScheduledType(t, controlled.Mgr, runID, "example.c", 1)
		completeContractActivity(t, controlled.Server, controlled.Activities, "example.c")
		waitForRunConfigErrorForTest(t, controlled.Mgr, runID)
		paused, err := controlled.Mgr.Get(runID)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(paused.ConfigError, "example.d") || !strings.Contains(paused.ConfigError, "input expected") {
			t.Fatalf("ConfigError = %q, want a d input contract mismatch", paused.ConfigError)
		}
		history := mgrEvents(t, controlled.Mgr, runID)
		if got := countScheduledType(history, "example.d"); got != 0 {
			t.Fatalf("d scheduled %d times while contract mismatch, want 0", got)
		}

		// 人工恢复：worker 上报 d 的新输入，再走 set-policy 入口。
		registerContractWorkerReporting(t, controlled.Server, false, true)
		if _, err := controlled.Server.SetRunPolicy(context.Background(), protocol.DefaultNamespace, runID, execution.RunPolicyAutoUpgrade); err != nil {
			t.Fatal(err)
		}
		waitForScheduledType(t, controlled.Mgr, runID, "example.d", 1)
		completeContractActivity(t, controlled.Server, controlled.Activities, "example.d")
		completeContractActivity(t, controlled.Server, controlled.Activities, "example.e")
		waitForRunStatusWithin(t, controlled.Server, runID, historypkg.StatusCompleted, 20*time.Second)
		assertRunVersion(t, controlled.Mgr, runID, refB.Version, refA.Version, "recovered next-wft run")
	})
}

// declaredRefForTest 从 WorkflowConfig 的 WLang Activities 映射里取指定 type 的声明。
func declaredRefForTest(t *testing.T, wf *program.WorkflowConfig, activityType string) program.ActivityRef {
	t.Helper()
	for _, ref := range wf.Activities {
		if ref.Type == activityType {
			return program.ActivityRef{Type: ref.Type, Input: ref.Input, Output: ref.Output}
		}
	}
	t.Fatalf("activity %s not declared in workflow config", activityType)
	return program.ActivityRef{}
}

// registerContractWorkerReporting 上报 b、c、d、e 的契约；cNote、dNote 为 true 时 c、d 的输入带 note 字段。
func registerContractWorkerReporting(t *testing.T, server *engine.Server, cNote, dNote bool) {
	t.Helper()
	b, err := activity.ContractOf[contractWorkerIn, contractWorkerOut]("example.b")
	if err != nil {
		t.Fatal(err)
	}
	c, err := activity.ContractOf[contractWorkerCIn, contractWorkerCOut]("example.c")
	if cNote {
		c, err = activity.ContractOf[contractWorkerCInWithNote, contractWorkerCOut]("example.c")
	}
	if err != nil {
		t.Fatal(err)
	}
	d, err := activity.ContractOf[contractWorkerDIn, contractWorkerDOut]("example.d")
	if dNote {
		d, err = activity.ContractOf[contractWorkerDInWithNote, contractWorkerDOut]("example.d")
	}
	if err != nil {
		t.Fatal(err)
	}
	e, err := activity.ContractOf[contractWorkerEIn, contractWorkerEOut]("example.e")
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RegisterActivityWorkerVersion(context.Background(), protocol.ActivityWorkerVersion{
		Identity: "contract-worker", BuildID: "contract-build",
		TaskQueues:    []string{"contract-replay"},
		ActivityTypes: []string{"example.b", "example.c", "example.d", "example.e"},
		Contracts:     []activity.Contract{b, c, d, e},
	}); err != nil {
		t.Fatal(err)
	}
}

func waitForRunConfigErrorForTest(t *testing.T, mgr *memory.ExecutionManager, runID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run, err := mgr.Get(runID)
		if err == nil && run.ConfigError != "" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	run, _ := mgr.Get(runID)
	if events, err := mgr.GetEvents(runID); err == nil {
		for _, event := range events {
			if event.WorkflowTaskFailed != nil {
				t.Logf("WFT failed: cause=%s reason=%s", event.WorkflowTaskFailed.Cause, event.WorkflowTaskFailed.Reason)
			}
			if event.ActivityScheduled != nil {
				t.Logf("scheduled: %s", event.ActivityScheduled.ActivityType)
			}
		}
	}
	t.Fatalf("run %s ConfigError stayed empty (status=%s upgrade=%q)", runID, run.Status, run.UpgradeError)
}

// 内容发布包含“完全没有 worker 路由”的 Activity type 时，发布成功，部署顺序不作要求。
func TestPublishAcceptsUnroutedActivityType(t *testing.T) {
	ctx := context.Background()
	server, mgr, _ := newUpgradeReplayServer(t)
	workflowID := "publish-unrouted-type"

	// 至少一个活动 worker 让 route catalog 生效，覆盖 A 的 Step1-4，但没有 Step5。
	registerTargetWorker(t, server, "upgrade-replay-worker", "build-v1", false)
	registerGeneratedWorkflowNamed(t, server, workflowID, upgradeSourceA, execution.RunPolicyAutoUpgrade, "A")

	generated, err := sdkgen.GenerateFromSource([]byte(upgradeSourceAppendE), sdkgen.Options{
		WorkflowType: workflowID, Name: "B", FuncName: "Flow", Filename: "flow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	wfB := generated.Workflow
	wfB.Policy = execution.RunPolicyAutoUpgrade
	refB, err := server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, wfB)
	if err != nil {
		t.Fatalf("publishing content with an unrouted activity type must succeed: %v", err)
	}

	runID, err := startedRunID(server.StartWorkflow(ctx, "", workflowID, workflowID, "", nil))
	if err != nil {
		t.Fatal(err)
	}
	run, err := mgr.Get(runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.WorkflowConfigVersion != refB.Version {
		t.Fatalf("new run uses %q, want latest %q", run.WorkflowConfigVersion, refB.Version)
	}
}

// completeContractActivity 以 registerContractWorkerReporting 登记的 Worker 身份从 contract-replay 领取下一个
// Activity Task，核对类型后完成它。
func completeContractActivity(t *testing.T, server *engine.Server, router *memory.ActivityQueueRouter, activityType string) {
	t.Helper()
	worker := protocol.WorkerRoutingInfo{Namespace: protocol.DefaultNamespace, Identity: "contract-worker", BuildID: "contract-build"}
	ctx, cancel := context.WithTimeout(protocol.WithWorkerRoutingInfo(protocol.WithNamespace(context.Background(), protocol.DefaultNamespace), worker), 10*time.Second)
	defer cancel()
	lease, err := router.Dequeue(ctx, "contract-replay")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Task.ActivityType != activityType {
		t.Fatalf("activity type = %q, want %q (step %s)", lease.Task.ActivityType, activityType, lease.Task.ActivityID)
	}
	completeActivityTask(t, server, lease.Task, map[string]any{})
}
