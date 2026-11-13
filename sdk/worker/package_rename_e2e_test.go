package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/donutnomad/tenuto/compiler/artifact"
	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/internal/executor/interpreter"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/workflow"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// ============================================
// 只改包名的升级
// ============================================
// 测试策略:
// - Happy Path: Workflow 与它的类型从包 order 移到 order222（@Workflow id、@Activity type、JSON 字段不变），
//   生成器产出新的内容版本；类型表的名字随包名改变，Activity 契约指纹不变
// - Happy Path: 停在 Signal 上的 pinned Run 留在旧 Program 完成；auto_upgrade Run 之后的 Workflow Task 换到
//   新 Program，用 order222 的类型表解码 order 写下的 History（Activity 结果、子 Workflow 结果、struct Signal）
//   并正确完成；两份 History 用各自实际采用的 Program 重放无非确定性错误
// - Happy Path: 同一个带粘性执行缓存的 Worker 跨过升级：auto_upgrade Run 的 cache key 随内容版本改变
// - Edge Cases（反例）: 新版本同时把 JSON 字段 order_id 改成 orderId：auto_upgrade Run 换版前的载荷检查发现
//   History 里 approve Signal 的 order_id 在新类型里不存在，Run 留在旧 Program 以 ConfigError 暂停，不产出错误结果；
//   pinned Run 的 Activity 契约只有已下线的旧 build 满足，Run 以 ConfigError 暂停。旧 build 重新上线、发布保留
//   order_id 的版本后，auto_upgrade Run 恢复并换到这个版本完成，pinned Run 恢复后留在旧 Program 完成
// ============================================

const renameWorkflowSrc = `package %[1]s

import (
	"context"
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type OrderInput struct {
	OrderID string ` + "`json:\"order_id\"`" + `
	Amount  int64  ` + "`json:\"amount\"`" + `
}

type ChargeInput struct {
	OrderID string ` + "`json:\"%[2]s\"`" + `
	Amount  int64  ` + "`json:\"amount\"`" + `
}

type ChargeOutput struct {
	ChargeID string ` + "`json:\"charge_id\"`" + `
}

type ChildInput struct {
	OrderID  string ` + "`json:\"order_id\"`" + `
	ChargeID string ` + "`json:\"charge_id\"`" + `
}

type ChildOutput struct {
	Receipt string ` + "`json:\"receipt\"`" + `
}

type Approval struct {
	OrderID  string ` + "`json:\"%[2]s\"`" + `
	Approver string ` + "`json:\"approver\"`" + `
}

type Result struct {
	OrderID     string ` + "`json:\"order_id\"`" + `
	FirstCharge string ` + "`json:\"first_charge\"`" + `
	Receipt     string ` + "`json:\"receipt\"`" + `
	Approver    string ` + "`json:\"approver\"`" + `
	FinalCharge string ` + "`json:\"final_charge\"`" + `
}

// @Activity(type="rename.charge",queue="rename",version=v1)
func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error) {
	return ChargeOutput{}, nil
}

// @Workflow(id="rename-order")
func Order(ctx workflow.Context, in OrderInput) (Result, error) {
	first, err := CallCharge(ctx, ChargeInput{OrderID: in.OrderID, Amount: in.Amount}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return Result{}, err
	}
	var child ChildOutput
	if err := workflow.ExecuteChildWorkflow(ctx, "rename-child", ChildInput{OrderID: in.OrderID, ChargeID: first.ChargeID}).Get(ctx, &child); err != nil {
		return Result{}, err
	}
	var approval Approval
	if err := workflow.WaitSignal(ctx, "approve").Get(ctx, &approval); err != nil {
		return Result{}, err
	}
	var shipped map[string]any
	if err := workflow.WaitSignal(ctx, "ship").Get(ctx, &shipped); err != nil {
		return Result{}, err
	}
	final, err := CallCharge(ctx, ChargeInput{OrderID: approval.OrderID, Amount: in.Amount + 1}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return Result{}, err
	}
	return Result{OrderID: in.OrderID, FirstCharge: first.ChargeID, Receipt: child.Receipt, Approver: approval.Approver, FinalCharge: final.ChargeID}, nil
}
`

const renameChildSrc = `package child

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func Child(ctx workflow.Context, input map[string]any) (map[string]any, error) {
	id, _ := input["order_id"].(string)
	charge, _ := input["charge_id"].(string)
	return map[string]any{"receipt": "receipt-" + id + "-" + charge}, nil
}
`

// Worker 侧的 Activity 实现用测试自己的 Go 类型：契约只看 JSON 形状，与 Go 包名、类型名无关。
type renameChargeIn struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

type renameChargeInCamel struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

type renameChargeOut struct {
	ChargeID string `json:"charge_id"`
}

func renameCharge(reg *activity.Registry) error {
	return activity.RegisterActivity(reg, "rename.charge", "v1", func(_ context.Context, in renameChargeIn) (renameChargeOut, error) {
		return renameChargeOut{ChargeID: fmt.Sprintf("charge-%s-%d", in.OrderID, in.Amount)}, nil
	})
}

func renameChargeCamel(reg *activity.Registry) error {
	return activity.RegisterActivity(reg, "rename.charge", "v1", func(_ context.Context, in renameChargeInCamel) (renameChargeOut, error) {
		return renameChargeOut{ChargeID: fmt.Sprintf("charge-%s-%d", in.OrderID, in.Amount)}, nil
	})
}

// renameArtifacts 是一次测试用到的全部生成产物。
type renameArtifacts struct {
	v1, v2, v2Camel      *program.WorkflowConfig
	v1Version, v2Version string
	v2CamelVersion       string
}

func TestPackageRenameUpgradeKeepsRunsWorking(t *testing.T) {
	arts := generateRenameArtifacts(t)

	// 生成结果：新的内容版本；类型表以 <import path>.<类型名> 命名，随包移动改名；Activity 契约不变。
	if arts.v1Version == arts.v2Version {
		t.Fatalf("package rename kept content version %s", arts.v1Version)
	}
	for _, name := range []string{"OrderInput", "ChargeInput", "ChildInput", "Approval", "Result"} {
		if !hasTypeSuffix(arts.v1, "/order."+name) || !hasTypeSuffix(arts.v2, "/order222."+name) {
			t.Errorf("type %s is not named after its package: v1 %v, v2 %v", name, typeNames(arts.v1), typeNames(arts.v2))
		}
	}
	for _, name := range typeNames(arts.v2) {
		if strings.Contains(name, "/order.") {
			t.Errorf("v2 type table still names %s", name)
		}
	}
	v1Refs, v2Refs := activityRefsByType(arts.v1), activityRefsByType(arts.v2)
	if len(v1Refs) != 1 || v1Refs["rename.charge"].Input == "" {
		t.Fatalf("v1 activities = %+v", arts.v1.Activities)
	}
	if v1Refs["rename.charge"] != v2Refs["rename.charge"] {
		t.Fatalf("activity contract changed with the package name: v1 %+v, v2 %+v", v1Refs, v2Refs)
	}
	contract, err := activity.ContractOf[renameChargeIn, renameChargeOut]("rename.charge", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if contract.Input != v1Refs["rename.charge"].Input || contract.Output != v1Refs["rename.charge"].Output {
		t.Fatalf("worker contract %+v differs from generated %+v", contract, v1Refs["rename.charge"])
	}
	if v2Refs["rename.charge"].Input == activityRefsByType(arts.v2Camel)["rename.charge"].Input {
		t.Fatal("renaming the JSON field kept the activity input fingerprint")
	}

	t.Run("redeployed worker", func(t *testing.T) {
		env := startRenameEnv(t)
		w1 := env.startWorker(t, "rename-w1", "b1", arts.v1, renameCharge, nil)
		auto, pinned := env.parkRuns(t)
		w1.Stop()
		env.startWorker(t, "rename-w2", "b2", arts.v2, renameCharge, nil)
		env.shipAndCheck(t, arts, auto, pinned)
	})

	t.Run("sticky execution cache across the upgrade", func(t *testing.T) {
		env := startRenameEnv(t)
		cache, err := executor.NewExecutionCache(executor.ExecutionCacheConfig{MaxEntries: 16, MaxEstimatedBytes: 16 << 20, IdleTTL: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		env.startWorker(t, "rename-sticky", "b1", arts.v1, renameCharge, cache)
		auto, pinned := env.parkRuns(t)
		before := cache.Snapshot()
		if before.Entries < 2 {
			t.Fatalf("parked runs are not cached: %+v", before)
		}
		// 同一个 Worker 进程继续运行，v2 由发布入口（generate -publish）激活为 latest。
		if _, err := (artifact.Client{BaseURL: env.url, Namespace: protocol.DefaultNamespace}).Submit(env.ctx, artifact.PublishPath, artifact.Artifacts{Workflow: arts.v2}); err != nil {
			t.Fatal(err)
		}
		env.shipAndCheck(t, arts, auto, pinned)
		after := cache.Snapshot()
		// pinned Run 的 key 不变，继续命中缓存。auto_upgrade Run 的 key 含新内容版本：查找落空一次，按完整 History
		// 用 v2 恢复后以新 key 入缓存。两个 Run 结束时各自当前 key 的条目被逐出；auto_upgrade Run 旧 key 的 v1
		// 条目不再被访问，留到 IdleTTL 或容量淘汰。
		if after.Hits <= before.Hits {
			t.Errorf("pinned run did not hit the cache: before %+v after %+v", before, after)
		}
		if after.Misses-before.Misses != 1 || after.Admissions-before.Admissions != 1 {
			t.Errorf("auto_upgrade run did not restore under a new key exactly once: before %+v after %+v", before, after)
		}
		if after.Entries != 1 || after.Evictions[protocol.ExecutionCacheEvictionInvalid] != 2 {
			t.Errorf("cache entries after both runs closed: %+v", after)
		}
	})

	t.Run("renamed JSON field", func(t *testing.T) {
		env := startRenameEnv(t)
		w1 := env.startWorker(t, "rename-w1", "b1", arts.v1, renameCharge, nil)
		auto, pinned := env.parkRuns(t)
		w1.Stop()
		// 新 build 的 Activity 实现随源码一起把字段改成 orderId，上报新契约。
		env.startWorker(t, "rename-w2", "b2", arts.v2Camel, renameChargeCamel, nil)
		if err := env.c.SignalWorkflow(env.ctx, auto, "", "ship", map[string]any{}); err != nil {
			t.Fatal(err)
		}
		if err := env.c.SignalWorkflow(env.ctx, pinned, "", "ship", map[string]any{}); err != nil {
			t.Fatal(err)
		}

		// auto_upgrade：History 里 approve 的载荷是 {"order_id": ...}，新类型只有 orderId。换版前的载荷检查拒绝升级，
		// Run 留在 v1 并以 ConfigError 暂停，消息写出位置、新旧类型与字段路径；没有任何命令在 v2 上执行。
		description := env.waitConfigError(t, auto)
		for _, want := range []string{
			"auto upgrade to " + arts.v2CamelVersion + " refused",
			`signal "approve": order.Approval -> order222.Approval: order_id: field is missing in the new type (removed or renamed)`,
		} {
			if !strings.Contains(description.ConfigError, want) {
				t.Fatalf("config error = %q, want it to contain %q", description.ConfigError, want)
			}
		}
		if description.ProgramVersion != arts.v1Version || description.Status != client.WorkflowExecutionStatusRunning {
			t.Fatalf("paused run: version %s status %s", description.ProgramVersion, description.Status)
		}
		for _, event := range env.history(t, auto) {
			if started := event.WorkflowTaskStarted; started != nil && started.ProgramVersion != arts.v1Version {
				t.Fatalf("workflow task %d ran %s", event.ID, started.ProgramVersion)
			}
		}

		// pinned：旧 Program 声明的 order_id 契约只有已停止的 b1 满足，最后一次扣款调度时 Run 以 ConfigError 暂停。
		description = env.waitConfigError(t, pinned)
		if !strings.Contains(description.ConfigError, `activity "rename.charge" contract mismatch`) {
			t.Fatalf("config error = %q", description.ConfigError)
		}
		if description.ProgramVersion != arts.v1Version {
			t.Fatalf("pinned run moved to %s", description.ProgramVersion)
		}

		// 修复：旧 build 重新上线（同一份 v1 定义再次登记不改变 latest），发布保留 order_id 的 v2。auto_upgrade 恢复后
		// 换到 v2 并用正确的载荷完成；pinned 恢复后留在 v1 完成。
		env.startWorker(t, "rename-w1-back", "b1", arts.v1, renameCharge, nil)
		if _, err := (artifact.Client{BaseURL: env.url, Namespace: protocol.DefaultNamespace}).Submit(env.ctx, artifact.PublishPath, artifact.Artifacts{Workflow: arts.v2}); err != nil {
			t.Fatal(err)
		}
		for _, item := range []struct {
			id     string
			policy workflow.Policy
			want   string
		}{{auto, workflow.PolicyAutoUpgrade, arts.v2Version}, {pinned, workflow.PolicyPinned, arts.v1Version}} {
			if err := env.c.UpdateWorkflowExecutionOptions(env.ctx, client.UpdateWorkflowExecutionOptions{WorkflowID: item.id, Policy: item.policy}); err != nil {
				t.Fatal(err)
			}
			assertJSONEqual(t, env.result(t, item.id), expectedRenameResult(item.id))
			env.assertProgramVersion(t, item.id, item.policy, item.want)
		}
	})
}

// generateRenameArtifacts 用真实生成器（扫描注解 → 写桥文件 → 翻译）从源码生成 order、order222 与改了 JSON 字段的
// order222 三个包。包目录建在模块内的 testdata 下（packages.Load 需要模块解析），测试结束时删除。
func generateRenameArtifacts(t *testing.T) renameArtifacts {
	t.Helper()
	if err := os.MkdirAll("testdata", 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("testdata", "tmp-rename-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(root)
		_ = os.Remove("testdata") // 只在目录已空时删除
	})
	generate := func(dir, pkg, orderIDField string) *program.WorkflowConfig {
		t.Helper()
		dir = filepath.Join(root, dir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "order.go"), fmt.Appendf(nil, renameWorkflowSrc, pkg, orderIDField), 0o644); err != nil {
			t.Fatal(err)
		}
		plan, err := sdkgen.Scan(dir)
		if err != nil {
			t.Fatalf("scan %s: %v", pkg, err)
		}
		for _, bridge := range plan.Bridges {
			if err := os.WriteFile(bridge.Path, bridge.Content, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		result, err := sdkgen.Generate(plan)
		if err != nil {
			t.Fatalf("generate %s: %v", pkg, err)
		}
		return result.Workflow
	}
	arts := renameArtifacts{
		v1:      generate("order", "order", "order_id"),
		v2:      generate("order222", "order222", "order_id"),
		v2Camel: generate("camel/order222", "order222", "orderId"),
	}
	for _, item := range []struct {
		config *program.WorkflowConfig
		out    *string
	}{{arts.v1, &arts.v1Version}, {arts.v2, &arts.v2Version}, {arts.v2Camel, &arts.v2CamelVersion}} {
		if *item.out, err = program.WorkflowConfigVersion(item.config); err != nil {
			t.Fatal(err)
		}
	}
	return arts
}

func activityRefsByType(config *program.WorkflowConfig) map[string]program.ActivityRef {
	refs := map[string]program.ActivityRef{}
	for _, ref := range config.SymbolRefs() {
		ref.Symbol = "" // 符号是 <包名>.<函数名>，随包名改变；按 type 对照契约
		refs[ref.Type] = ref
	}
	return refs
}

type renameEnv struct {
	url string
	c   client.Client
	ctx context.Context
}

func startRenameEnv(t *testing.T) *renameEnv {
	t.Helper()
	server := assembly.NewServer(assembly.Options{
		Stores:        assembly.Stores{Executions: memory.NewExecutionManager()},
		WorkflowQueue: memory.NewTaskQueue(64), ActivityRouter: memory.NewActivityQueueRouter(64),
	})
	serverCtx, cancelServer := context.WithCancel(context.Background())
	httpServer := httptest.NewUnstartedServer(runtimehttp.NewRuntimeHTTPHandler(server))
	httpServer.Config.BaseContext = func(net.Listener) context.Context { return serverCtx }
	httpServer.Start()
	c, err := client.Dial(client.Options{HostPort: httpServer.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// Cleanup 后进先出：先停 Worker（startWorker 注册的），再关 Client 与 Server。
	t.Cleanup(func() {
		cancel()
		c.Close()
		cancelServer()
		httpServer.Close()
		server.Stop()
	})
	return &renameEnv{url: httpServer.URL, c: c, ctx: ctx}
}

// startWorker 启动一个同时执行 Workflow 与 Activity 的 Worker，随二进制登记 parent 与子 Workflow 定义。
func (e *renameEnv) startWorker(t *testing.T, identity, buildID string, parent *program.WorkflowConfig, register func(*activity.Registry) error, cache *executor.ExecutionCache) *Worker {
	t.Helper()
	registry := activity.NewRegistry()
	if err := register(registry); err != nil {
		t.Fatal(err)
	}
	w, err := newWorker(e.c, Options{
		Workflows:          []registration.Definition{renameDefinition(t, parent), renameDefinition(t, mustChild(t))},
		Activities:         registry,
		ActivityTaskQueues: []string{"rename"},
		Identity:           identity,
		BuildID:            buildID,
	}, cache)
	if err != nil {
		t.Fatal(err)
	}
	w.Start(e.ctx)
	t.Cleanup(w.Stop)
	if err := w.WaitReady(e.ctx); err != nil {
		t.Fatal(err)
	}
	return w
}

var renameChildConfig *program.WorkflowConfig

func mustChild(t *testing.T) *program.WorkflowConfig {
	t.Helper()
	if renameChildConfig == nil {
		child, err := sdkgen.GenerateFromSource([]byte(renameChildSrc), sdkgen.Options{WorkflowID: "rename-child", FuncName: "Child", Filename: "child.go"})
		if err != nil {
			t.Fatal(err)
		}
		renameChildConfig = child.Workflow
	}
	return renameChildConfig
}

func renameDefinition(t *testing.T, config *program.WorkflowConfig) registration.Definition {
	t.Helper()
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return registration.Definition{FS: fstest.MapFS{registration.ArtifactFile: &fstest.MapFile{Data: data}}}
}

// parkRuns 在 v1 上启动两个 Run，等它们执行完第一次扣款、子 Workflow 与 approve Signal，停在 ship Signal 上；
// 第二个 Run 改为 pinned。
func (e *renameEnv) parkRuns(t *testing.T) (auto, pinned string) {
	t.Helper()
	auto, pinned = "rename-auto", "rename-pinned"
	for _, id := range []string{auto, pinned} {
		if _, err := e.c.ExecuteWorkflow(e.ctx, client.StartWorkflowOptions{ID: id}, "rename-order", map[string]any{"order_id": id, "amount": 100}); err != nil {
			t.Fatal(err)
		}
		if err := e.c.SignalWorkflow(e.ctx, id, "", "approve", map[string]any{"order_id": id + "-approved", "approver": "alice"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{auto, pinned} {
		e.waitParked(t, id)
	}
	if err := e.c.UpdateWorkflowExecutionOptions(e.ctx, client.UpdateWorkflowExecutionOptions{WorkflowID: pinned, Policy: workflow.PolicyPinned}); err != nil {
		t.Fatal(err)
	}
	// 改策略调度了一个 Workflow Task；等它完成，调用方停 Worker 时没有在途的 Workflow Task。
	e.waitParked(t, pinned)
	return auto, pinned
}

// waitParked 等到 History 里已有子 Workflow 的结果与 approve Signal，且最后一个事件是完成的 Workflow Task。
func (e *renameEnv) waitParked(t *testing.T, workflowID string) {
	t.Helper()
	for {
		events := e.history(t, workflowID)
		childDone, approved := false, false
		for _, event := range events {
			childDone = childDone || event.ChildWorkflowCompleted != nil
			approved = approved || (event.WorkflowSignaled != nil && event.WorkflowSignaled.Name == "approve")
		}
		if childDone && approved && events[len(events)-1].WorkflowTaskCompleted != nil {
			return
		}
		select {
		case <-e.ctx.Done():
			t.Fatalf("%s did not park: %d events", workflowID, len(events))
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (e *renameEnv) history(t *testing.T, workflowID string) []historypkg.Event {
	t.Helper()
	var events []historypkg.Event
	for it := e.c.GetWorkflowHistory(e.ctx, workflowID, ""); it.HasNext(); {
		event, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, *event)
	}
	return events
}

func (e *renameEnv) result(t *testing.T, workflowID string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := e.c.GetWorkflow(e.ctx, workflowID, "").Get(e.ctx, &out); err != nil {
		t.Fatalf("%s: %v", workflowID, err)
	}
	return out
}

func (e *renameEnv) waitConfigError(t *testing.T, workflowID string) *client.WorkflowExecutionDescription {
	t.Helper()
	for {
		description, err := e.c.DescribeWorkflowExecution(e.ctx, workflowID, "")
		if err != nil {
			t.Fatal(err)
		}
		if description.ConfigError != "" {
			return description
		}
		select {
		case <-e.ctx.Done():
			t.Fatalf("%s has no config error: %+v", workflowID, description)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (e *renameEnv) assertProgramVersion(t *testing.T, workflowID string, policy workflow.Policy, version string) {
	t.Helper()
	description, err := e.c.DescribeWorkflowExecution(e.ctx, workflowID, "")
	if err != nil {
		t.Fatal(err)
	}
	if description.Policy != policy || description.ProgramVersion != version {
		t.Fatalf("%s: policy %s version %s, want %s %s", workflowID, description.Policy, description.ProgramVersion, policy, version)
	}
}

func expectedRenameResult(workflowID string) map[string]any {
	return map[string]any{
		"order_id":     workflowID,
		"first_charge": "charge-" + workflowID + "-100",
		"receipt":      "receipt-" + workflowID + "-charge-" + workflowID + "-100",
		"approver":     "alice",
		"final_charge": "charge-" + workflowID + "-approved-101",
	}
}

// shipAndCheck 发出 ship Signal，断言两个 Run 的结果、各自采用的内容版本，并用它们实际采用的 Program 重放 History。
func (e *renameEnv) shipAndCheck(t *testing.T, arts renameArtifacts, auto, pinned string) {
	t.Helper()
	for _, id := range []string{auto, pinned} {
		if err := e.c.SignalWorkflow(e.ctx, id, "", "ship", map[string]any{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{auto, pinned} {
		assertJSONEqual(t, e.result(t, id), expectedRenameResult(id))
	}
	e.assertProgramVersion(t, auto, workflow.PolicyAutoUpgrade, arts.v2Version)
	e.assertProgramVersion(t, pinned, workflow.PolicyPinned, arts.v1Version)

	// auto_upgrade：ship 之前的 Workflow Task 在 v1 上，之后的在 v2 上；pinned 全部在 v1 上。
	autoEvents := e.history(t, auto)
	shipped := false
	for _, event := range autoEvents {
		if event.WorkflowSignaled != nil && event.WorkflowSignaled.Name == "ship" {
			shipped = true
		}
		if started := event.WorkflowTaskStarted; started != nil {
			want := arts.v1Version
			if shipped {
				want = arts.v2Version
			}
			if started.ProgramVersion != want {
				t.Errorf("%s: workflow task %d ran %s, want %s", auto, event.ID, started.ProgramVersion, want)
			}
		}
	}
	pinnedEvents := e.history(t, pinned)
	for _, event := range pinnedEvents {
		if started := event.WorkflowTaskStarted; started != nil && started.ProgramVersion != arts.v1Version {
			t.Errorf("%s: workflow task %d ran %s, want %s", pinned, event.ID, started.ProgramVersion, arts.v1Version)
		}
	}
	replayRename(t, arts.v2, autoEvents, expectedRenameResult(auto))
	replayRename(t, arts.v1, pinnedEvents, expectedRenameResult(pinned))
	// 两份 Program 只差类型名，对方的 History 同样重放通过。
	replayRename(t, arts.v1, autoEvents, expectedRenameResult(auto))
	replayRename(t, arts.v2, pinnedEvents, expectedRenameResult(pinned))
}

// replayRename 用 config 从头重放到最后一个完成的 Workflow Task 之前，要求产出与 Run 结果相同的完成命令。
func replayRename(t *testing.T, config *program.WorkflowConfig, events []historypkg.Event, want map[string]any) {
	t.Helper()
	last := -1
	for i, event := range events {
		if event.WorkflowTaskCompleted != nil {
			last = i
		}
	}
	commands, err := interpreter.NewWorkflowExecutor(config, events[:last]).Execute()
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(commands) != 1 || commands[0].Type != historypkg.CmdCompleteWorkflow {
		t.Fatalf("replay produced %+v, want the completion", commands)
	}
	got, err := json.Marshal(commands[0].Output)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, decoded, want)
}

func assertJSONEqual(t *testing.T, got, want map[string]any) {
	t.Helper()
	gotJSON, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("got %s\nwant %s", gotJSON, wantJSON)
	}
}

func typeNames(config *program.WorkflowConfig) []string {
	var names []string
	for name := range config.Program.Types {
		names = append(names, name)
	}
	return names
}

func hasTypeSuffix(config *program.WorkflowConfig, suffix string) bool {
	for name := range config.Program.Types {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}
